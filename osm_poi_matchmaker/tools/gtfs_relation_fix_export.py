# -*- coding: utf-8 -*-
"""Generate .osm modify-diffs that complete existing OSM PT route relations with
their missing GTFS stops - both stops that already have a matching OSM node
(just not yet a member of this relation) and stops that have no OSM node at
all yet (a brand-new node is created for them, in the same file, and added as
a member right alongside).

This is phase 5 of the route reconciliation task, scoped to "fix membership",
not "fix/verify the way geometry" (phase 4/6, not attempted here - the way
members of each relation are carried over completely unchanged).

Positioning a missing stop within the member list does NOT require matching
the relation's way geometry: the relation's OWN existing member order is used
as the backbone, and the GTFS stop_sequence of whichever already-included
stops bracket the missing one (its "anchors") decides where to insert it. A
relation with fewer than one usable anchor is skipped and reported instead of
guessed at.

Depends on osm_poi_matchmaker.tools.gtfs_route_audit for the GTFS-side data
(canonical per-route stop sequences, the GTFS-stop -> OSM-node map) and the
OSM-side route relation inventory. Writes nothing to the live database or to
OSM - only to local .osm files, one per fixed relation, for manual JOSM review
and upload. Run standalone:

    python3 -m osm_poi_matchmaker.tools.gtfs_relation_fix_export --feed hu_bkv --out-dir ./gtfs_relation_fixes
"""

import argparse
import logging
import os
import sys
import zipfile
from xml.sax.saxutils import escape

try:
    import sqlalchemy
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.tools.gtfs_route_audit import (
        FEEDS, _read_gtfs_csv, build_canonical_routes, get_osm_stop_map,
    )
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('gtfs_relation_fix_export')

# GTFS route_type -> tags for a brand-new stop node, when a GTFS stop has no
# matching OSM node at all yet. Mirrors the tagging scheme the matching
# dataprovider already uses for this mode (see hu_bkv.py's types()) - kept
# minimal here (just enough for a valid, reviewable PTv2 stop) rather than
# duplicating every brand/payment/etc. tag that provider carries; a later
# regular pipeline run will fill those in once this node gets its ref tag
# matched, same as any other already-tagged stop.
ROUTE_TYPE_STOP_TAGS = {
    '0': {'railway': 'tram_stop', 'public_transport': 'platform', 'tram': 'yes'},  # tram
    '1': {'railway': 'stop', 'public_transport': 'platform', 'subway': 'yes'},  # subway/metro
    '2': {'railway': 'stop', 'public_transport': 'platform', 'train': 'yes'},  # rail
    '3': {'highway': 'bus_stop', 'public_transport': 'platform', 'bus': 'yes'},  # bus
    '4': {'amenity': 'ferry_terminal', 'public_transport': 'platform'},  # ferry
}


def get_osm_route_relations_full(engine):
    """[{'id', 'tags', 'members': [(type, ref, role), ...]}] for every OSM
    type=route/route_master relation - members kept in their original order
    (unlike gtfs_route_audit.get_osm_route_relations(), which only needs the
    unordered stop-node set)."""
    query = sqlalchemy.text("SELECT id, members, tags FROM planet_osm_rels WHERE tags && ARRAY['route']")
    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()
    relations = []
    for row in rows:
        tags = row['tags'] or []
        tag_dict = dict(zip(tags[0::2], tags[1::2]))
        if tag_dict.get('type') not in ('route', 'route_master'):
            continue
        raw_members = row['members'] or []
        members = []
        for i in range(0, len(raw_members) - 1, 2):
            ref = raw_members[i]
            role = raw_members[i + 1]
            if not ref:
                continue
            mtype = {'n': 'node', 'w': 'way', 'r': 'relation'}.get(ref[0], 'unknown')
            try:
                mref = int(ref[1:])
            except ValueError:
                continue
            members.append((mtype, mref, role))
        relations.append({'id': row['id'], 'tags': tag_dict, 'members': members})
    return relations


def get_gtfs_stop_info(zf):
    """{stop_id: (stop_name, lat, lon)} from stops.txt."""
    stops = _read_gtfs_csv(zf, 'stops.txt', usecols=lambda c: c in ('stop_id', 'stop_name', 'stop_lat', 'stop_lon'))
    return {row.stop_id: (row.stop_name, row.stop_lat, row.stop_lon) for row in stops.itertuples()}


def build_insert_plan(stops_sequence, stop_map, members):
    """Decide, for one (route_id, direction_id) pattern already matched to a
    relation, where each missing GTFS stop should be inserted into that
    relation's member list.

    Args:
        stops_sequence: Ordered tuple of GTFS stop_ids for this route pattern.
        stop_map: {gtfs_stop_id: osm_node_id} (feed-wide).
        members: The relation's current ordered [(type, ref, role), ...].

    Returns:
        list[(insert_after_index, stop_id, osm_node_id | None)] in the order
        they should be applied (highest index first, so inserting doesn't
        shift earlier indices this plan still refers to), or None if there's
        no usable anchor (fewer than one existing member is a recognizable
        GTFS stop of this route) - too ambiguous to place anything safely.
    """
    member_node_to_index = {ref: idx for idx, (mtype, ref, role) in enumerate(members) if mtype == 'node'}
    # anchor: this route's stop_sequence position -> the relation member index
    # that already represents it.
    anchors = []  # [(seq_index, member_index)]
    for seq_index, stop_id in enumerate(stops_sequence):
        node_id = stop_map.get(stop_id)
        if node_id is not None and node_id in member_node_to_index:
            anchors.append((seq_index, member_node_to_index[node_id]))
    if not anchors:
        return None

    anchors.sort()
    plan = []
    for seq_index, stop_id in enumerate(stops_sequence):
        node_id = stop_map.get(stop_id)
        if node_id is not None and node_id in member_node_to_index:
            continue  # already a member, nothing to do
        # Find the nearest preceding anchor (by sequence) - insert right after
        # its member index. If the missing stop is before every anchor, insert
        # right before the first anchor's member index instead.
        preceding = [a for a in anchors if a[0] < seq_index]
        if preceding:
            insert_after_index = preceding[-1][1]
        else:
            insert_after_index = anchors[0][1] - 1
        plan.append((insert_after_index, stop_id, node_id))
    # Apply from the end backwards so earlier insert_after_index values still
    # point at the right place once later insertions have shifted the list.
    plan.sort(key=lambda p: p[0], reverse=True)
    return plan


def apply_plan(members, plan, stop_tags, new_node_start_id=-1):
    """Return (new_members, new_nodes) - new_members is `members` with the
    plan's stops spliced in, new_nodes is [{'tmp_id', 'stop_id', 'lat', 'lon',
    'tags'} ...] for stops that needed a brand-new node."""
    members = list(members)
    new_nodes = []
    next_tmp_id = new_node_start_id
    for insert_after_index, stop_id, node_id in plan:
        # Match whichever role the relation's own neighbouring stop/platform
        # members already use (BKK relations are almost always 'platform',
        # some others 'stop') - inserting a differently-roled member next to
        # otherwise-uniform neighbours would look like a tagging mistake to a
        # reviewer even though the membership itself is correct.
        role = members[insert_after_index][2] if 0 <= insert_after_index < len(members) else 'platform'
        if node_id is not None:
            ref = node_id
        else:
            info = stop_tags['stop_info'].get(stop_id)
            if info is None:
                continue
            name, lat, lon = info
            tmp_id = next_tmp_id
            next_tmp_id -= 1
            new_nodes.append({'tmp_id': tmp_id, 'stop_id': stop_id, 'name': name, 'lat': lat, 'lon': lon,
                              'tags': stop_tags['mode_tags']})
            ref = tmp_id
        members.insert(insert_after_index + 1, ('node', ref, role))
    return members, new_nodes


def write_relation_fix_osm(path, relation, new_members, new_nodes, feed, ref_tag_key):
    lines = ['<?xml version=\'1.0\' encoding=\'UTF-8\'?>', '<osm version=\'0.6\' generator=\'JOSM\'>']
    lines.append('  <!-- Relation {} ({}): {} missing stop(s) added, {} of them brand-new nodes -->'.format(
        relation['id'], escape(str(relation['tags'].get('name', ''))), len(new_members) - len(relation['members']),
        len(new_nodes)))
    for n in new_nodes:
        tag_xml = ''.join('\n    <tag k=\'{}\' v=\'{}\'/>'.format(escape(k), escape(str(v)))
                          for k, v in n['tags'].items())
        name_tag = '\n    <tag k=\'name\' v=\'{}\'/>'.format(escape(str(n['name']))) if n['name'] else ''
        ref_tag = '\n    <tag k=\'{}\' v=\'{}\'/>'.format(escape(ref_tag_key), escape(str(n['stop_id'])))
        lines.append('  <!-- GTFS stop {} has no matching OSM node yet - new node created at its raw '
                     'GTFS coordinate (not road-snapped here; review placement before uploading). -->'
                     .format(n['stop_id']))
        lines.append('  <node action=\'new\' id=\'{}\' lat=\'{}\' lon=\'{}\' user=\'osm_poi_matchmaker\' '
                     'uid=\'1\' version=\'1\'>{}{}{}\n  </node>'.format(
                         n['tmp_id'], n['lat'], n['lon'], tag_xml, name_tag, ref_tag))
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
    from osm_poi_matchmaker.tools.gtfs_route_audit import match_routes, get_osm_route_relations

    feed = FEEDS[feed_key]
    zip_path = '{}/{}'.format(cache_dir.rstrip('/'), feed['zip'])
    with zipfile.ZipFile(zip_path) as zf:
        routes, canon_df = build_canonical_routes(zf)
        stop_info = get_gtfs_stop_info(zf)
    stop_map = get_osm_stop_map(engine, feed['stop_ref_tag'])
    relations_summary = get_osm_route_relations(engine)
    results = match_routes(canon_df, stop_map, relations_summary, min_overlap=min_overlap)
    results = results.merge(routes[['route_id', 'route_type']], on='route_id', how='left')

    relations_full = {r['id']: r for r in get_osm_route_relations_full(engine)}

    feed_out_dir = os.path.join(out_dir, feed_key)
    os.makedirs(feed_out_dir, exist_ok=True)

    written, skipped_no_anchor, nothing_to_add = 0, 0, 0
    for _, row in results.iterrows():
        if row['note'] != 'matched':
            continue
        rel_id = row['osm_relation_id']
        relation = relations_full.get(rel_id)
        if relation is None:
            continue
        canon_row = canon_df[(canon_df.route_id == row['route_id'])
                             & (canon_df.direction_id == row['direction_id'])].iloc[0]
        plan = build_insert_plan(canon_row['stops'], stop_map, relation['members'])
        if plan is None:
            skipped_no_anchor += 1
            continue
        if not plan:
            nothing_to_add += 1
            continue
        mode_tags = ROUTE_TYPE_STOP_TAGS.get(str(row.get('route_type', '')), ROUTE_TYPE_STOP_TAGS['3'])
        new_members, new_nodes = apply_plan(relation['members'], plan,
                                            {'stop_info': stop_info, 'mode_tags': mode_tags})
        out_path = os.path.join(feed_out_dir, 'relation_{}.osm'.format(rel_id))
        write_relation_fix_osm(out_path, relation, new_members, new_nodes, feed_key, feed['stop_ref_tag'])
        written += 1

    log.info('[%s] %d relation fix file(s) written to %s (%d skipped - no usable anchor, '
            '%d had nothing to add)', feed_key, written, feed_out_dir, skipped_no_anchor, nothing_to_add)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--feed', choices=list(FEEDS) + ['all'], default='all')
    parser.add_argument('--min-overlap', type=float, default=0.5)
    parser.add_argument('--out-dir', default='gtfs_relation_fixes')
    args = parser.parse_args()

    cache_dir = config.get_directory_cache_url()
    engine = sqlalchemy.create_engine('{}://{}:{}@{}:{}/{}'.format(
        config.get_database_type(), config.get_database_writer_username(),
        config.get_database_writer_password(), config.get_database_writer_host(),
        config.get_database_writer_port(), config.get_database_poi_database()))

    feed_keys = list(FEEDS) if args.feed == 'all' else [args.feed]
    for feed_key in feed_keys:
        run_feed(feed_key, cache_dir, engine, args.out_dir, min_overlap=args.min_overlap)


if __name__ == '__main__':
    main()
