# -*- coding: utf-8 -*-
"""Check way-member continuity of OSM PT route relations matched to a GTFS
route (phase 6 of the route reconciliation task: verify, don't generate).

A route relation's way members should chain together into one continuous
line - each way sharing an endpoint node with the next - the same thing
JOSM's own PT route validator checks for. This never touches way geometry
itself (that's the harder, not-yet-attempted phase 4: assembling a route's
way chain from scratch via the GTFS shape for routes with NO existing
relation at all); it only verifies what the EXISTING relations already claim
to have, and reports exactly where the chain breaks.

Writes nothing - read-only report (CSV) only. Run standalone:

    python3 -m osm_poi_matchmaker.tools.gtfs_relation_continuity_check --feed hu_bkv
"""

import argparse
import csv
import logging
import sys
import zipfile
from collections import defaultdict

try:
    import pandas as pd
    import sqlalchemy
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.tools.gtfs_route_audit import (
        FEEDS, build_canonical_routes, get_feed_stop_map, get_osm_route_relations, match_routes,
    )
    from osm_poi_matchmaker.tools.gtfs_relation_fix_export import get_osm_route_relations_full
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('gtfs_relation_continuity_check')


def get_way_nodesets(engine, way_ids):
    """{way_id: set(node_id, ...)} for the given way ids, from planet_osm_ways'
    own node-sequence array (osm2pgsql slim-mode middle table) - the only
    place a way's node order survives; planet_osm_line only has geometry, not
    which OSM nodes it passes through.

    The full node set is needed, not just the first/last node: a closed way
    (a junction=roundabout, routinely part of a bus/rail route) connects to
    its neighbours at whichever ring node they actually meet, which is almost
    never the way's own first==last node - checking endpoints only reports a
    roundabout as broken on both sides even when it's mapped perfectly fine
    (confirmed live: a Volánbusz relation's roundabout member looked broken
    purely because of this, with real shared nodes a few positions in).
    """
    if not way_ids:
        return {}
    query = sqlalchemy.text('SELECT id, nodes FROM planet_osm_ways WHERE id = ANY(:ids)')
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'ids': list(way_ids)})
    return {row.id: set(row.nodes) for row in df.itertuples()}


def check_relation_continuity(members, nodesets):
    """Returns a list of break descriptions - empty if the way chain is fully
    continuous (or has fewer than 2 way members, nothing to check).

    Each break is (position, way_a, way_b): way_a and way_b (adjacent 'way'
    members, ignoring any node/relation members between them) share no node
    at all, so the route visually/physically jumps between them. Checking
    full node sets (not just each way's own first/last node) is required for
    a closed way (a roundabout) to be recognised as connecting correctly at
    whichever ring node its neighbours actually meet it at.
    """
    way_members = [ref for mtype, ref, role in members if mtype == 'way']
    breaks = []
    for i in range(len(way_members) - 1):
        way_a, way_b = way_members[i], way_members[i + 1]
        nodes_a = nodesets.get(way_a)
        nodes_b = nodesets.get(way_b)
        if nodes_a is None or nodes_b is None:
            breaks.append((i, way_a, way_b, 'way not found in planet_osm_ways (deleted/outdated local extract?)'))
            continue
        if not (nodes_a & nodes_b):
            breaks.append((i, way_a, way_b, 'no shared node'))
    return breaks


def run_feed(feed_key, cache_dir, engine, min_overlap=0.5):
    feed = FEEDS[feed_key]
    zip_path = '{}/{}'.format(cache_dir.rstrip('/'), feed['zip'])
    with zipfile.ZipFile(zip_path) as zf:
        routes, canon_df = build_canonical_routes(zf)
        stop_map = get_feed_stop_map(feed_key, feed, zf, engine)
    relations_summary = get_osm_route_relations(engine)
    results = match_routes(canon_df, stop_map, relations_summary, min_overlap=min_overlap)
    matched_ids = set(results.loc[results.note == 'matched', 'osm_relation_id'].dropna().astype(int))

    relations_full = {r['id']: r for r in get_osm_route_relations_full(engine) if r['id'] in matched_ids}
    all_way_ids = {ref for rel in relations_full.values() for mtype, ref, role in rel['members'] if mtype == 'way'}
    log.info('[%s] Checking way continuity for %d matched relations (%d distinct ways)',
            feed_key, len(relations_full), len(all_way_ids))
    nodesets = get_way_nodesets(engine, all_way_ids)

    rows = []
    for rel_id, relation in relations_full.items():
        breaks = check_relation_continuity(relation['members'], nodesets)
        # A "way not found in planet_osm_ways" break isn't a real routing gap -
        # it just means that way's nodes are outside this (Hungary-only)
        # osm2pgsql extract, which happens routinely for the Hungarian end of a
        # cross-border line (confirmed live: an Austria-Hungary rail relation
        # reports 247/270 "breaks", every one of them this reason, not a real
        # discontinuity). Only count "no shared node" as a real gap.
        real_gaps = [b for b in breaks if b[3] == 'no shared node']
        missing_way = [b for b in breaks if b[3] != 'no shared node']
        way_count = sum(1 for mtype, ref, role in relation['members'] if mtype == 'way')
        rows.append({
            'osm_relation_id': rel_id, 'name': relation['tags'].get('name', ''),
            'ref': relation['tags'].get('ref', ''), 'way_count': way_count,
            'real_gap_count': len(real_gaps),
            'missing_way_count': len(missing_way),
            'first_real_gap': '{}/{} (pos {})'.format(real_gaps[0][1], real_gaps[0][2], real_gaps[0][0])
            if real_gaps else '',
        })
    columns = ['osm_relation_id', 'name', 'ref', 'way_count', 'real_gap_count', 'missing_way_count',
              'first_real_gap']
    df = pd.DataFrame(rows, columns=columns)  # explicit columns: a feed with 0 matched relations
                                              # (e.g. no OSM coverage at all yet) would otherwise
                                              # build a columnless DataFrame from an empty row list.
    broken = (df['real_gap_count'] > 0).sum()
    partly_outside_extract = (df['missing_way_count'] > 0).sum()
    log.info('[%s] %d/%d matched relations have a real way-chain gap (%d more have ways outside this '
            'Hungary-only extract, e.g. cross-border lines - not counted as gaps)',
            feed_key, broken, len(df), partly_outside_extract)
    return df


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--feed', choices=list(FEEDS) + ['all'], default='all')
    parser.add_argument('--min-overlap', type=float, default=0.5)
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    cache_dir = config.get_directory_cache_url()
    engine = sqlalchemy.create_engine('{}://{}:{}@{}:{}/{}'.format(
        config.get_database_type(), config.get_database_writer_username(),
        config.get_database_writer_password(), config.get_database_writer_host(),
        config.get_database_writer_port(), config.get_database_poi_database()))

    feed_keys = list(FEEDS) if args.feed == 'all' else [args.feed]
    for feed_key in feed_keys:
        df = run_feed(feed_key, cache_dir, engine, min_overlap=args.min_overlap)
        out_path = args.out or 'gtfs_relation_continuity_{}.csv'.format(feed_key)
        df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
        log.info('[%s] Report written to %s', feed_key, out_path)


if __name__ == '__main__':
    main()
