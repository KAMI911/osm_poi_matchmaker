# -*- coding: utf-8 -*-
"""Read-only GTFS <-> OSM public transport route audit.

Phases 1-3 of the route reconciliation task: for each configured GTFS feed,
build each route's canonical stop sequence from the already-downloaded GTFS
cache zip, map each GTFS stop to the OSM node the stop-level matcher already
linked it to (via that node's ref:bkk / gtfs:stop_id:HU-<feed> / ref:mav tag),
then check which GTFS routes already have a corresponding OSM type=route
relation (by stop-set overlap) and which don't.

This writes nothing to OSM, to any pipeline table, or to the live database -
it only reads the cached GTFS zips and the local osm2pgsql-imported database,
and prints/saves a CSV report. Run standalone:

    python3 -m osm_poi_matchmaker.tools.gtfs_route_audit --feed hu_bkv
    python3 -m osm_poi_matchmaker.tools.gtfs_route_audit --feed all
"""

import argparse
import csv
import logging
import sys
import zipfile
from collections import Counter, defaultdict

try:
    import pandas as pd
    import sqlalchemy
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('gtfs_route_audit')

# One entry per supported feed: the cached zip filename (as already downloaded by
# the matching dataprovider - see hu_bkv.py/hu_volanbusz.py/hu_mav.py) and the OSM
# tag that holds this feed's GTFS stop_id on an already-matched stop node (see
# each provider's additional_ref_name / the stop-level matcher).
#
# hu_helyi_kozlekedes's per-city feeds are deliberately left out for now: none of
# their stops currently carry a gtfs:stop_id:HU-<CITY> tag in the live OSM data
# (checked directly - every one of them is 0 rows), so there is no stop-level
# anchor to match routes against yet; route matching for those would have to
# fall back to name/network heuristics alone, which isn't reliable enough to
# report a real coverage number.
FEEDS = {
    'hu_bkv': {'zip': 'hu_bkv.zip', 'stop_ref_tag': 'ref:bkk'},
    'hu_volanbusz': {'zip': 'hu_volanbusz.zip', 'stop_ref_tag': 'gtfs:stop_id:HU-VOLAN'},
    'hu_mav': {'zip': 'hu_mav.zip', 'stop_ref_tag': 'ref:mav'},
}


def _read_gtfs_csv(zf, name, usecols=None):
    with zf.open(name) as raw:
        return pd.read_csv(raw, usecols=usecols, dtype=str, low_memory=False)


def build_canonical_routes(zf):
    """Return (routes_df, canon_df). canon_df has one row per (route_id,
    direction_id) with that pattern's most common ordered stop_id tuple among
    this route's trips, how many trips share it, and how many trips the route
    has in total."""
    routes = _read_gtfs_csv(zf, 'routes.txt')
    trips = _read_gtfs_csv(zf, 'trips.txt',
                           usecols=lambda c: c in ('route_id', 'trip_id', 'direction_id', 'shape_id'))
    stop_times = _read_gtfs_csv(zf, 'stop_times.txt',
                                usecols=lambda c: c in ('trip_id', 'stop_id', 'stop_sequence'))
    stop_times['stop_sequence'] = pd.to_numeric(stop_times['stop_sequence'], errors='coerce')
    stop_times = stop_times.sort_values(['trip_id', 'stop_sequence'])
    trip_stops = stop_times.groupby('trip_id')['stop_id'].apply(tuple)

    # Not trips.set_index('trip_id'); Index.map(dict-of-tuples) misreads the tuple
    # values as MultiIndex level components and blows up on ragged tuple lengths -
    # Series.apply() with a plain lookup function doesn't have that special case.
    trip_stops_map = trip_stops.to_dict()
    trips['stops'] = trips['trip_id'].apply(trip_stops_map.get)
    trips = trips.dropna(subset=['stops'])
    if 'direction_id' not in trips.columns:
        trips['direction_id'] = ''
    trips['direction_id'] = trips['direction_id'].fillna('')

    canon_rows = []
    for (route_id, direction_id), group in trips.groupby(['route_id', 'direction_id']):
        pattern_counts = Counter(group['stops'])
        best_pattern, freq = pattern_counts.most_common(1)[0]
        canon_rows.append({
            'route_id': route_id, 'direction_id': direction_id,
            'stops': best_pattern, 'pattern_trip_count': freq, 'route_trip_count': len(group),
        })
    canon_df = pd.DataFrame(canon_rows)
    return routes, canon_df


def get_osm_stop_map(engine, stop_ref_tag):
    """{gtfs_stop_id: osm_node_id} for every OSM node carrying stop_ref_tag."""
    query = sqlalchemy.text(
        'SELECT osm_id, "{col}" AS ref FROM planet_osm_point WHERE "{col}" IS NOT NULL'.format(col=stop_ref_tag))
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    # A stop_id could in principle map to more than one node (rare tagging
    # duplicates) - last one wins, good enough for an overlap-based audit.
    return dict(zip(df['ref'], df['osm_id']))


def get_osm_route_relations(engine):
    """[{'id', 'tags', 'stop_node_ids'}] for every OSM type=route/route_master
    relation - tags parsed from planet_osm_rels' flat tags array (same storage
    as its members array, see poi_base.py's relationer()), stop_node_ids the
    set of plain OSM node ids (abs value) among its members."""
    query = sqlalchemy.text("SELECT id, members, tags FROM planet_osm_rels WHERE tags && ARRAY['route']")
    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()
    relations = []
    for row in rows:
        tags = row['tags'] or []
        tag_dict = dict(zip(tags[0::2], tags[1::2]))
        if tag_dict.get('type') not in ('route', 'route_master'):
            continue
        members = row['members'] or []
        node_ids = set()
        for i in range(0, len(members) - 1, 2):
            ref = members[i]
            if ref and ref[0] == 'n':
                try:
                    node_ids.add(int(ref[1:]))
                except ValueError:
                    pass
        relations.append({'id': row['id'], 'tags': tag_dict, 'stop_node_ids': node_ids})
    return relations


def match_routes(canon_df, stop_map, relations, min_overlap=0.5):
    """For each canonical (route_id, direction_id) row, find the OSM relation
    with the best stop-set overlap: |matched GTFS stops in that relation| /
    |GTFS stops that have ANY matching OSM node at all|.

    Uses a node->candidate-relations inverted index instead of comparing every
    route against every relation (14k+ relations nationwide x thousands of
    GTFS routes would be a slow full cross product otherwise).
    """
    node_to_rels = defaultdict(list)
    for rel in relations:
        for n in rel['stop_node_ids']:
            node_to_rels[n].append(rel)
    relations_by_id = {rel['id']: rel for rel in relations}

    results = []
    for _, row in canon_df.iterrows():
        gtfs_osm_nodes = {stop_map[s] for s in row['stops'] if s in stop_map}
        base = row.to_dict()
        if not gtfs_osm_nodes:
            results.append({**base, 'osm_relation_id': None, 'overlap': 0.0,
                            'note': 'no GTFS stop on this route has a matching OSM node at all'})
            continue
        candidate_ids = set()
        for n in gtfs_osm_nodes:
            for rel in node_to_rels.get(n, []):
                candidate_ids.add(rel['id'])
        best_id, best_score = None, 0.0
        for rel in relations:
            if rel['id'] not in candidate_ids:
                continue
            inter = gtfs_osm_nodes & rel['stop_node_ids']
            if not inter:
                continue
            score = len(inter) / len(gtfs_osm_nodes)
            if score > best_score:
                best_score, best_id = score, rel['id']
        note = 'matched' if best_id is not None and best_score >= min_overlap \
            else 'no OSM relation with enough stop overlap'
        entry = {**base, 'osm_relation_id': best_id, 'overlap': round(best_score, 3), 'note': note}
        # Phase 5 diagnostic: for a matched relation, name exactly what's wrong -
        # which GTFS stops (in route order) already have an OSM node but aren't
        # among the relation's own members yet, versus stops that have no OSM
        # node at all (those need the stop itself matched/created first - adding
        # them to the relation isn't the next step for those).
        stops_without_osm_node = [s for s in row['stops'] if s not in stop_map]
        if best_id is not None:
            rel_nodes = relations_by_id[best_id]['stop_node_ids']
            missing_in_relation = [(s, stop_map[s]) for s in row['stops']
                                   if s in stop_map and stop_map[s] not in rel_nodes]
        else:
            missing_in_relation = []
        entry['missing_osm_stop_ids'] = ';'.join(str(n) for _, n in missing_in_relation)
        entry['missing_in_relation_count'] = len(missing_in_relation)
        entry['stops_without_osm_node_count'] = len(stops_without_osm_node)
        results.append(entry)
    return pd.DataFrame(results)


def run_feed(feed_key, cache_dir, engine, min_overlap=0.5):
    feed = FEEDS[feed_key]
    zip_path = '{}/{}'.format(cache_dir.rstrip('/'), feed['zip'])
    log.info('[%s] Reading GTFS zip: %s', feed_key, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        routes, canon_df = build_canonical_routes(zf)
    log.info('[%s] %d routes, %d canonical (route_id, direction_id) patterns', feed_key, len(routes), len(canon_df))

    log.info('[%s] Loading OSM stop map (tag %s) ...', feed_key, feed['stop_ref_tag'])
    stop_map = get_osm_stop_map(engine, feed['stop_ref_tag'])
    log.info('[%s] %d OSM nodes carry this feed\'s stop ref tag', feed_key, len(stop_map))

    log.info('[%s] Loading OSM route/route_master relations ...', feed_key)
    relations = get_osm_route_relations(engine)
    log.info('[%s] %d OSM type=route/route_master relations nationwide', feed_key, len(relations))

    results = match_routes(canon_df, stop_map, relations, min_overlap=min_overlap)
    results = results.merge(routes[['route_id', 'route_short_name', 'route_long_name', 'route_type']],
                            on='route_id', how='left')

    matched = (results['note'] == 'matched').sum()
    total = len(results)
    log.info('[%s] %d/%d (route_id, direction_id) patterns matched to an existing OSM relation (overlap >= %.0f%%)',
            feed_key, matched, total, min_overlap * 100)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--feed', choices=list(FEEDS) + ['all'], default='all')
    parser.add_argument('--min-overlap', type=float, default=0.5,
                        help='Minimum fraction of a route\'s matched-to-OSM stops that must fall inside a '
                             'candidate relation for it to count as a match (default 0.5).')
    parser.add_argument('--out', default=None,
                        help='CSV output path (default: gtfs_route_audit_<feed>.csv in the current directory).')
    args = parser.parse_args()

    cache_dir = config.get_directory_cache_url()
    engine = sqlalchemy.create_engine('{}://{}:{}@{}:{}/{}'.format(
        config.get_database_type(),
        config.get_database_writer_username(),
        config.get_database_writer_password(),
        config.get_database_writer_host(),
        config.get_database_writer_port(),
        config.get_database_poi_database()))

    feed_keys = list(FEEDS) if args.feed == 'all' else [args.feed]
    for feed_key in feed_keys:
        results = run_feed(feed_key, cache_dir, engine, min_overlap=args.min_overlap)
        out_path = args.out or 'gtfs_route_audit_{}.csv'.format(feed_key)
        results.drop(columns=['stops']).to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
        log.info('[%s] Report written to %s', feed_key, out_path)


if __name__ == '__main__':
    main()
