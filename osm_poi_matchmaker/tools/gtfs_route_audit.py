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
FEEDS = {
    'hu_bkv': {'zip': 'hu_bkv.zip', 'stop_ref_tag': 'ref:bkk'},
    'hu_volanbusz': {'zip': 'hu_volanbusz.zip', 'stop_ref_tag': 'gtfs:stop_id:HU-VOLAN'},
    'hu_mav': {'zip': 'hu_mav.zip', 'stop_ref_tag': 'ref:mav'},
}

# hu_helyi_kozlekedes's per-city feeds, one FEEDS entry each (hu_helyi_<slug>) -
# kept separate from the dict above since the zip filename and stop ref tag
# both vary per city (see hu_helyi_kozlekedes.py's CITY_FEEDS/additional_ref_name).
#
# As of this writing every one of these tags is 0 rows in the live OSM data
# (checked directly), so running the audit against the CURRENT database will
# report ~100% of these routes as having "no GTFS stop on this route has a
# matching OSM node at all" - that's not a bug in the audit, it's the real
# state: hu_helyi_kozlekedes.py's own poi_search_name bug (fixed, but only in
# the next pipeline run - see its own commit) was silently preventing these
# stops from ever matching. These entries exist so the tooling needs no
# further changes once a future run has actually matched some.
_HELYI_KOZLEKEDES_CITY_SLUGS = (
    'pecs', 'miskolc', 'szeged', 'szekesfehervar', 'szombathely', 'veszprem',
    'tatabanya', 'kaposvar', 'dombovar', 'hodmezovasarhely', 'kiskunhalas',
    'paks', 'varpalota', 'budaors', 'bahart', 'weekendbus',
)
for _slug in _HELYI_KOZLEKEDES_CITY_SLUGS:
    FEEDS['hu_helyi_{}'.format(_slug)] = {
        'zip': 'hu_helyi_kozlekedes_{}.zip'.format(_slug),
        'stop_ref_tag': 'gtfs:stop_id:HU-{}'.format(_slug.upper()),
    }
del _slug


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


def get_gtfs_stop_info(zf):
    """{stop_id: (stop_name, lat, lon)} from stops.txt."""
    stops = _read_gtfs_csv(zf, 'stops.txt', usecols=lambda c: c in ('stop_id', 'stop_name', 'stop_lat', 'stop_lon'))
    return {row.stop_id: (row.stop_name, row.stop_lat, row.stop_lon) for row in stops.itertuples()}


def get_osm_stop_map(engine, stop_ref_tag):
    """{gtfs_stop_id: osm_node_id} for every OSM node carrying stop_ref_tag."""
    query = sqlalchemy.text(
        'SELECT osm_id, "{col}" AS ref FROM planet_osm_point WHERE "{col}" IS NOT NULL'.format(col=stop_ref_tag))
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    # A stop_id could in principle map to more than one node (rare tagging
    # duplicates) - last one wins, good enough for an overlap-based audit.
    return dict(zip(df['ref'], df['osm_id']))


def get_osm_stop_map_by_distance(engine, stops_info, distance_m=50):
    """{gtfs_stop_id: osm_node_id} built by nearest-node lookup instead of a
    ref tag - for a feed (e.g. hu_helyi_kozlekedes's per-city ones) whose
    stops carry no ref tag in OSM yet at all, so get_osm_stop_map() alone
    would find nothing. This mirrors the matcher's own last-resort,
    name-less distance stage (poi_base.py) directly against raw GTFS
    coordinates, so route-level work on these feeds doesn't have to wait for
    an upload+reimport cycle just to get a first, rough coverage estimate -
    it's not a substitute for the real stop-level matching+upload (no ref
    tag gets written anywhere by this), just a way to see where things stand
    today.

    Args:
        stops_info: {stop_id: (name, lat, lon)} as returned by
            gtfs_relation_fix_export.get_gtfs_stop_info().
        distance_m: Search radius in meters - kept tight on purpose (this is
            a name-less, type-only match, the same risk profile as
            poi_base.py's own 10-15m last-resort stage for these modes; a
            little looser here since GTFS coordinates for small local feeds
            tend to be less precise than BKK/Volánbusz's).
    """
    if not stops_info:
        return {}
    rows = [(stop_id, float(lat), float(lon)) for stop_id, (_, lat, lon) in stops_info.items()
           if lat and lon]
    if not rows:
        return {}
    query = sqlalchemy.text('''
        WITH stops(stop_id, lat, lon) AS (
            SELECT * FROM unnest(:stop_ids, :lats, :lons)
        )
        SELECT s.stop_id, nearest.osm_id
        FROM stops s,
        LATERAL (
            SELECT osm_id, ST_DistanceSphere(way, ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)) AS dist
            FROM planet_osm_point
            WHERE highway = 'bus_stop' OR railway IN ('tram_stop', 'stop') OR amenity = 'ferry_terminal'
            ORDER BY way <-> ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)
            LIMIT 1
        ) nearest
        WHERE nearest.dist < :distance_m
    ''')
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'stop_ids': [r[0] for r in rows], 'lats': [r[1] for r in rows], 'lons': [r[2] for r in rows],
            'distance_m': distance_m,
        })
    return dict(zip(df['stop_id'], df['osm_id']))


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


def get_feed_stop_map(feed_key, feed, zf, engine, distance_fallback=True, distance_fallback_m=50):
    """{gtfs_stop_id: osm_node_id} for one feed: the ref-tag map
    (get_osm_stop_map()), topped up with a distance-based fallback
    (get_osm_stop_map_by_distance()) for whichever stops have no ref tag at
    all yet - every caller that needs a feed's stop map (the audit itself,
    the relation fix exporters, the continuity/gap-fix tools) goes through
    this one place so they all see the same, consistently-built map instead
    of each reimplementing the ref-tag-vs-fallback merge slightly differently.
    """
    log.info('[%s] Loading OSM stop map (tag %s) ...', feed_key, feed['stop_ref_tag'])
    stop_map = get_osm_stop_map(engine, feed['stop_ref_tag'])
    log.info('[%s] %d OSM nodes carry this feed\'s stop ref tag', feed_key, len(stop_map))
    if distance_fallback:
        # For stops with no ref tag at all yet (routine for a feed no one has
        # uploaded matches for yet, e.g. hu_helyi_kozlekedes's cities), fall
        # back to a live, name-less nearest-node lookup against their raw
        # GTFS coordinate - see get_osm_stop_map_by_distance()'s docstring for
        # why this is a rough estimate, not a substitute for the real
        # (ref-tag-writing) stop-level match.
        stop_info = get_gtfs_stop_info(zf)
        missing_stop_info = {s: info for s, info in stop_info.items() if s not in stop_map}
        if missing_stop_info:
            fallback_map = get_osm_stop_map_by_distance(engine, missing_stop_info, distance_m=distance_fallback_m)
            if fallback_map:
                log.info('[%s] +%d more stops matched by distance fallback (no ref tag yet, within %dm)',
                        feed_key, len(fallback_map), distance_fallback_m)
                stop_map = {**stop_map, **fallback_map}
    return stop_map


def run_feed(feed_key, cache_dir, engine, min_overlap=0.5, distance_fallback=True, distance_fallback_m=50):
    feed = FEEDS[feed_key]
    zip_path = '{}/{}'.format(cache_dir.rstrip('/'), feed['zip'])
    log.info('[%s] Reading GTFS zip: %s', feed_key, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        routes, canon_df = build_canonical_routes(zf)
        stop_map = get_feed_stop_map(feed_key, feed, zf, engine, distance_fallback, distance_fallback_m)
    log.info('[%s] %d routes, %d canonical (route_id, direction_id) patterns', feed_key, len(routes), len(canon_df))

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
