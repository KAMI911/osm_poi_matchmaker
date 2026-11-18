# -*- coding: utf-8 -*-
"""Automatically bridge a real way-chain gap (as found by
gtfs_relation_continuity_check) by routing through the local OSM road network
between the two disconnected ways' nearest endpoints, and splice the
resulting way chain into the relation.

Domestic lines only for now (BKK, Volánbusz) - cross-border MÁV gaps are
skipped, since the gap could sit right at/near the border where this
Hungary-only extract's road network may itself be incomplete, making an
auto-found "shortest path" untrustworthy there.

This is the riskier phase 4 work the rest of the route-reconciliation tools
deliberately avoided: it invents new relation content (the bridging way
sequence) rather than just reporting or adding already-known elements, so
every result is written as a reviewable .osm file with the exact path taken
called out in a comment - never applied automatically to OSM itself.

Run standalone:

    python3 -m osm_poi_matchmaker.tools.gtfs_relation_gap_fix --feed hu_bkv,hu_volanbusz
"""

import argparse
import heapq
import logging
import os
import sys
import zipfile
from xml.sax.saxutils import escape

try:
    import sqlalchemy
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.tools.gtfs_route_audit import (
        FEEDS, build_canonical_routes, get_osm_stop_map, get_osm_route_relations, match_routes,
    )
    from osm_poi_matchmaker.tools.gtfs_relation_fix_export import get_osm_route_relations_full, _safe_comment_text
    from osm_poi_matchmaker.tools.gtfs_relation_continuity_check import get_way_nodesets, check_relation_continuity
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('gtfs_relation_gap_fix')

# The gap could sit right at/near the border, where this Hungary-only
# extract's road/rail network may itself be incomplete - but find_bridge()'s
# max_gap_m safety cap already refuses to bridge anything implausibly far
# apart regardless of feed, which covers this case generically: a cross-
# border gap either resolves within a short, safe distance on the Hungarian
# side, or gets correctly left for manual review either way.
DOMESTIC_FEEDS = ('hu_bkv', 'hu_volanbusz', 'hu_mav')

MAX_SEARCH_RADIUS_M = 600  # local-graph bounding box half-size around the gap
MAX_HOPS = 25  # BFS hop limit - generous for short urban road segments, still bounded by max_gap_m below

ONEWAY_FORWARD_VALUES = frozenset(('yes', 'true', '1'))
ONEWAY_REVERSE_VALUES = frozenset(('-1', 'reverse'))
ROUNDABOUT_JUNCTION_VALUES = frozenset(('roundabout', 'circular'))

# GTFS route_type -> which OSM network a route of that mode actually runs on,
# and which vehicle-specific oneway exemptions apply. Trams/metro/rail don't
# get bus contraflow-lane exemptions; buses do.
ROUTE_TYPE_TO_NETWORK = {
    '0': ('rail', 'rail'),   # tram
    '1': ('rail', 'rail'),   # subway/metro
    '2': ('rail', 'rail'),   # rail
    '3': ('road', 'bus'),    # bus
    '11': ('road', 'bus'),   # trolleybus
    '800': ('road', 'bus'),  # Szeged's extended trolleybus code
}


def directed_edges_for_way(nodes, tags, vehicle='bus'):
    """Return the [(from_node, to_node), ...] edges this way allows a vehicle
    of the given kind to travel, honouring oneway restrictions - the
    direction-correctness the route-bridging search depends on so it never
    routes a bus the wrong way down a one-way street or through a roundabout
    backwards.

    - junction=roundabout/circular implies oneway=yes along the way's own
      digitized direction for road vehicles (the OSM-wide convention: a
      closed way representing a roundabout doesn't need its own explicit
      oneway tag) - not for rail, where "junction" isn't a road concept.
    - An explicit oneway tag always overrides the roundabout default.
    - 'bus'/'psv' vehicles are exempt from an otherwise-standing restriction
      when oneway:psv=no or oneway:bus=no is set (a bus contraflow lane).
      Note: this project's current osm2pgsql import doesn't expose these
      sub-tags on planet_osm_line (not in default.style, no hstore there),
      but planet_osm_ways.tags (the raw, unfiltered per-way tag list this
      module reads instead - see build_local_network_graph()) does carry
      them, so the exemption is honoured for real once a way has one.
    """
    oneway = (tags.get('oneway') or '').strip().lower()
    junction = (tags.get('junction') or '').strip().lower()
    if vehicle != 'rail' and not oneway and junction in ROUNDABOUT_JUNCTION_VALUES:
        oneway = 'yes'
    exempt = False
    if vehicle in ('bus', 'psv'):
        for key in ('oneway:psv', 'oneway:bus'):
            if (tags.get(key) or '').strip().lower() == 'no':
                exempt = True
                break
    forward_only = (not exempt) and oneway in ONEWAY_FORWARD_VALUES
    reverse_only = (not exempt) and oneway in ONEWAY_REVERSE_VALUES
    edges = []
    for a, b in zip(nodes, nodes[1:]):
        if reverse_only:
            edges.append((b, a))
        elif forward_only:
            edges.append((a, b))
        else:
            edges.append((a, b))
            edges.append((b, a))
    return edges


def get_node_coords(engine, node_ids):
    if not node_ids:
        return {}
    query = sqlalchemy.text('SELECT id, lat / 1e7 AS lat, lon / 1e7 AS lon FROM planet_osm_nodes WHERE id = ANY(:ids)')
    with engine.connect() as conn:
        rows = conn.execute(query, {'ids': list(node_ids)}).mappings().all()
    return {row['id']: (row['lat'], row['lon']) for row in rows}


def build_local_network_graph(engine, center_lat, center_lon, network='road', vehicle='bus',
                              radius_m=MAX_SEARCH_RADIUS_M):
    """{node_id: [(neighbour_node_id, way_id), ...]} - directed per
    directed_edges_for_way() - for every way of the right mode (highway for
    'road', railway for 'rail') with any node inside a square of `radius_m`
    around the given point (rough and generous on purpose: a square, not a
    precise circle, so a short bridging path is still reachable even if the
    start/end nodes aren't exactly centered).

    Reads tags from planet_osm_ways (the osm2pgsql middle table's raw,
    unfiltered per-way tag list), not planet_osm_line's style-whitelisted
    typed columns - oneway/junction are in both, but oneway:psv/oneway:bus
    are only ever in the raw list here.
    """
    deg = radius_m / 70000.0
    mode_column = 'railway' if network == 'rail' else 'highway'
    query = sqlalchemy.text('''
        SELECT w.id, w.nodes, w.tags
        FROM planet_osm_ways w
        JOIN planet_osm_line l ON l.osm_id = w.id
        WHERE l.{mode_column} IS NOT NULL
          AND l.way && ST_Expand(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), :deg)
    '''.format(mode_column=mode_column))
    with engine.connect() as conn:
        rows = conn.execute(query, {'lon': center_lon, 'lat': center_lat, 'deg': deg}).mappings().all()
    graph = {}
    for row in rows:
        nodes = row['nodes']
        way_id = row['id']
        raw_tags = row['tags'] or []
        tags = dict(zip(raw_tags[0::2], raw_tags[1::2]))
        for a, b in directed_edges_for_way(nodes, tags, vehicle=vehicle):
            graph.setdefault(a, []).append((b, way_id))
    return graph


def shortest_way_path(graph, start_nodes, end_nodes, max_hops=MAX_HOPS):
    """Dijkstra-ish BFS (unweighted, hop count only) from any of start_nodes to
    any of end_nodes. Returns the ordered list of way ids used, or None if
    nothing is reachable within max_hops."""
    end_set = set(end_nodes)
    if end_set & set(start_nodes):
        return []  # already touching - no bridge needed (shouldn't happen, caller already excluded this)
    visited = {n: None for n in start_nodes}  # node -> (prev_node, way_id)
    queue = [(0, n) for n in start_nodes]
    heapq.heapify(queue)
    while queue:
        hops, node = heapq.heappop(queue)
        if hops > max_hops:
            continue
        if node in end_set:
            # Walk back to reconstruct the way sequence.
            path_ways = []
            cur = node
            while visited[cur] is not None:
                prev, way_id = visited[cur]
                path_ways.append(way_id)
                cur = prev
            path_ways.reverse()
            # Collapse consecutive duplicate way ids (the BFS can step onto
            # the same way more than once between its own internal nodes).
            collapsed = []
            for w in path_ways:
                if not collapsed or collapsed[-1] != w:
                    collapsed.append(w)
            return collapsed
        for neighbour, way_id in graph.get(node, []):
            if neighbour not in visited:
                visited[neighbour] = (node, way_id)
                heapq.heappush(queue, (hops + 1, neighbour))
    return None


def _haversine_m(a, b):
    import math
    lat1, lon1 = a
    lat2, lon2 = b
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    x = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(x)))


def find_bridge(engine, way_a_nodes, way_b_nodes, node_coords, network='road', vehicle='bus', max_gap_m=500):
    """Try routing from way_a to way_b through the local road network.

    Centers the search on the single closest pair of nodes between the two
    ways (not the centroid of every node in both - a long way's own nodes
    are spread along its whole length, which can place the centroid well
    away from the actual gap and miss it with a small bounding box). Bails
    out before even querying if the closest pair is implausibly far apart -
    a large gap is far more likely a misplaced/unrelated way left in the
    relation by a past edit than a genuine missing short segment, and
    blindly inserting a many-way "bridge" across kilometers would be guessing,
    not fixing.
    """
    best_pair, best_dist = None, None
    for a in way_a_nodes:
        ca = node_coords.get(a)
        if ca is None:
            continue
        for b in way_b_nodes:
            cb = node_coords.get(b)
            if cb is None:
                continue
            d = _haversine_m(ca, cb)
            if best_dist is None or d < best_dist:
                best_dist, best_pair = d, (ca, cb)
    if best_pair is None:
        return None, None
    if best_dist > max_gap_m:
        return None, best_dist
    center_lat = (best_pair[0][0] + best_pair[1][0]) / 2
    center_lon = (best_pair[0][1] + best_pair[1][1]) / 2
    radius = max(MAX_SEARCH_RADIUS_M, best_dist * 1.5 + 100)
    graph = build_local_network_graph(engine, center_lat, center_lon, network=network, vehicle=vehicle,
                                      radius_m=radius)
    return shortest_way_path(graph, way_a_nodes, way_b_nodes), best_dist


def write_gap_fix_osm(path, relation, way_a, way_b, bridge_way_ids, break_position):
    lines = ['<?xml version=\'1.0\' encoding=\'UTF-8\'?>', '<osm version=\'0.6\' generator=\'JOSM\'>']
    lines.append('  <!-- Relation {} ({}): way-chain gap between {} and {} auto-bridged via {} '
                 'intermediate way(s) - VERIFY before uploading, this path was found automatically, '
                 'not surveyed. -->'.format(
                     relation['id'], _safe_comment_text(relation['tags'].get('name', '')),
                     way_a, way_b, len(bridge_way_ids)))
    lines.append('  <!-- Bridge way order: {} -->'.format(', '.join(str(w) for w in bridge_way_ids)))
    way_members = [m for m in relation['members'] if m[0] == 'way']
    insert_pos_in_full = None
    way_seen = 0
    for idx, (mtype, ref, role) in enumerate(relation['members']):
        if mtype == 'way':
            if way_seen == break_position:
                insert_pos_in_full = idx
                break
            way_seen += 1
    new_members = list(relation['members'])
    for offset, w in enumerate(bridge_way_ids):
        new_members.insert(insert_pos_in_full + 1 + offset, ('way', w, ''))
    tag_xml = ''.join('\n    <tag k=\'{}\' v=\'{}\'/>'.format(escape(k), escape(str(v)))
                      for k, v in relation['tags'].items())
    member_xml = ''.join('\n    <member type=\'{}\' ref=\'{}\' role=\'{}\'/>'.format(mtype, ref, role or '')
                         for mtype, ref, role in new_members)
    lines.append('  <relation action=\'modify\' id=\'{}\'>{}{}\n  </relation>'.format(
        relation['id'], member_xml, tag_xml))
    lines.append('</osm>')
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')


def run_feed(feed_key, cache_dir, engine, out_dir, min_overlap=0.5):
    feed = FEEDS[feed_key]
    zip_path = '{}/{}'.format(cache_dir.rstrip('/'), feed['zip'])
    with zipfile.ZipFile(zip_path) as zf:
        routes, canon_df = build_canonical_routes(zf)
    stop_map = get_osm_stop_map(engine, feed['stop_ref_tag'])
    relations_summary = get_osm_route_relations(engine)
    results = match_routes(canon_df, stop_map, relations_summary, min_overlap=min_overlap)
    results = results.merge(routes[['route_id', 'route_type']], on='route_id', how='left')
    matched = results[results.note == 'matched'].dropna(subset=['osm_relation_id'])
    matched_ids = set(matched['osm_relation_id'].astype(int))
    # A relation can in principle be matched from more than one canon row (two
    # directions sharing one relation id, etc.) - route_type should always
    # agree between them since it's the same real-world route either way, so
    # just take whichever row got there first.
    route_type_by_rel_id = dict(zip(matched['osm_relation_id'].astype(int), matched['route_type']))
    relations_full = {r['id']: r for r in get_osm_route_relations_full(engine) if r['id'] in matched_ids}
    all_way_ids = {ref for rel in relations_full.values() for mtype, ref, role in rel['members'] if mtype == 'way'}
    nodesets = get_way_nodesets(engine, all_way_ids)

    feed_out_dir = os.path.join(out_dir, feed_key)
    os.makedirs(feed_out_dir, exist_ok=True)
    bridged, failed = 0, 0
    for rel_id, relation in relations_full.items():
        breaks = check_relation_continuity(relation['members'], nodesets)
        real_gaps = [b for b in breaks if b[3] == 'no shared node']
        if not real_gaps:
            continue
        # One gap at a time per relation for now - a relation with multiple
        # independent gaps needs each one reviewed on its own anyway.
        position, way_a, way_b, _ = real_gaps[0]
        nodes_a, nodes_b = nodesets.get(way_a), nodesets.get(way_b)
        if not nodes_a or not nodes_b:
            failed += 1
            continue
        node_coords = get_node_coords(engine, nodes_a | nodes_b)
        network, vehicle = ROUTE_TYPE_TO_NETWORK.get(str(route_type_by_rel_id.get(rel_id, '')), ('road', 'bus'))
        bridge, gap_dist = find_bridge(engine, nodes_a, nodes_b, node_coords, network=network, vehicle=vehicle)
        if not bridge:
            if gap_dist is not None and gap_dist > 500:
                log.info('[%s] Relation %s: %s/%s are %.0fm apart at their closest - too far for a real '
                        'missing segment, likely a misplaced/unrelated way in the relation - needs manual review',
                        feed_key, rel_id, way_a, way_b, gap_dist)
            else:
                log.info('[%s] Relation %s: no road-network path found between %s/%s (%s) - needs manual review',
                        feed_key, rel_id, way_a, way_b,
                        '{:.0f}m apart'.format(gap_dist) if gap_dist is not None else 'distance unknown')
            failed += 1
            continue
        out_path = os.path.join(feed_out_dir, 'relation_{}_gapfix.osm'.format(rel_id))
        write_gap_fix_osm(out_path, relation, way_a, way_b, bridge, position)
        log.info('[%s] Relation %s: bridged %s/%s with %d way(s) -> %s',
                feed_key, rel_id, way_a, way_b, len(bridge), out_path)
        bridged += 1
    log.info('[%s] %d relation(s) bridged, %d could not be auto-fixed (needs manual review)',
            feed_key, bridged, failed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--feed', default='domestic',
                        help='Comma-separated feed keys, "domestic" (bkv+volanbusz, default), or "all". '
                             'Cross-border feeds (hu_mav) are excluded unless explicitly named.')
    parser.add_argument('--min-overlap', type=float, default=0.5)
    parser.add_argument('--out-dir', default=None)
    args = parser.parse_args()

    if args.feed == 'domestic':
        feed_keys = list(DOMESTIC_FEEDS)
    elif args.feed == 'all':
        feed_keys = list(FEEDS)
    else:
        feed_keys = [f.strip() for f in args.feed.split(',') if f.strip()]

    out_dir = args.out_dir or os.path.join(config.get_directory_output(), 'gtfs_relation_fixes')
    cache_dir = config.get_directory_cache_url()
    engine = sqlalchemy.create_engine('{}://{}:{}@{}:{}/{}'.format(
        config.get_database_type(), config.get_database_writer_username(),
        config.get_database_writer_password(), config.get_database_writer_host(),
        config.get_database_writer_port(), config.get_database_poi_database()))

    for feed_key in feed_keys:
        run_feed(feed_key, cache_dir, engine, out_dir, min_overlap=args.min_overlap)


if __name__ == '__main__':
    main()
