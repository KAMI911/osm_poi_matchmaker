# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import pandas as pd
    import numpy as np
    from math import radians, cos, sin, asin, sqrt
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)


def haversine(lon1, lat1, lon2, lat2):
    """Calculate great circle distance between two points on earth (in meters)."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6371 * c
    return km * 1000


def find_osm_id_conflicts(data):
    """Find all POIs that share the same osm_id (non-None) AND poi_type.

    Grouping by osm_id alone would flag two POIs of *different* types matched to
    the same OSM element (e.g. an Aldi shop and a parcel locker vending machine
    tagged on the same building/node) as a spurious duplicate - they're distinct,
    legitimate real-world features that both belong there, not two provider rows
    describing the same object. Only same-type matches to the same osm_id (the
    actual duplicate-harvest case this module exists for) count as a conflict.

    Args:
        data (pd.DataFrame): POI data with an osm_id column; also grouped by
            poi_type if that column is present (older/minimal callers - mostly in
            tests - that don't carry poi_type fall back to the osm_id-only
            behaviour).

    Returns:
        dict: {group_key: [list of row indices]} for groups with 2+ POIs. group_key
        is the bare osm_id if poi_type isn't a column, else an (osm_id, poi_type) tuple.
    """
    conflicts = {}
    matched = data[data['osm_id'].notna()]
    group_cols = ['osm_id', 'poi_type'] if 'poi_type' in data.columns else ['osm_id']
    group_sizes = matched.groupby(group_cols, dropna=False).size()
    for key, count in group_sizes[group_sizes > 1].items():
        mask = matched['osm_id'] == (key[0] if len(group_cols) > 1 else key)
        if len(group_cols) > 1:
            mask = mask & (matched['poi_type'] == key[1])
        conflict_rows = matched[mask].index.tolist()
        conflicts[key] = conflict_rows
    return conflicts


def get_available_osm_candidates(db_session, poi_data, search_distance=500):
    """Get nearby OSM elements that could be reassigned to this POI.

    Args:
        db_session: SQLAlchemy session
        poi_data: Single POI row
        search_distance: Distance in meters to search

    Returns:
        list: Available OSM element IDs sorted by distance
    """
    from osm_poi_matchmaker.dao.poi_base import POIBase

    if not poi_data['poi_lon'] or not poi_data['poi_lat']:
        return []

    try:
        nearby = POIBase.query_osm_shop_poi_gpd_impl(
            db_session.connection(),
            poi_data['poi_lon'],
            poi_data['poi_lat'],
            poi_data.get('poi_type', 'shop'),
            poi_data.get('poi_name'),
            poi_data.get('poi_avoid_name'),
            None, None, None,
            None, None, None,
            poi_data.get('poi_city'),
            distance_perfect=search_distance,
            distance_safe=search_distance,
            distance_unsafe=search_distance,
            with_metadata=False,
            limit=5
        )
        return nearby['osm_id'].tolist() if nearby is not None else []
    except Exception as e:
        logging.warning('Failed to query OSM candidates for POI: %s', e)
        return []


def resolve_conflict(data, group_key, conflict_indices, db_session=None):
    """Resolve a single OSM-element conflict by reassigning POIs to different OSM elements.

    Sorts conflicting POIs by distance to their current OSM element, then reassigns
    the farthest POI to the nearest available alternative OSM element.

    Args:
        data (pd.DataFrame): Full POI dataset
        group_key: The conflicted group's key - a bare osm_id, or (per
            find_osm_id_conflicts()) an (osm_id, poi_type) tuple; used only for
            logging here.
        conflict_indices (list): Row indices of conflicting POIs
        db_session: Database session for OSM queries (optional)

    Returns:
        bool: True if reassignment succeeded, False if no alternative found
    """
    if len(conflict_indices) < 2:
        return False

    conflict_data = data.loc[conflict_indices]

    distances = []
    for idx in conflict_indices:
        poi = data.loc[idx]
        if poi['poi_lon'] and poi['poi_lat']:
            try:
                osm_lon = float(poi.get('osm_lon', poi['poi_lon']))
                osm_lat = float(poi.get('osm_lat', poi['poi_lat']))
                dist = haversine(poi['poi_lon'], poi['poi_lat'], osm_lon, osm_lat)
                distances.append((idx, dist))
            except (TypeError, ValueError):
                distances.append((idx, float('inf')))
        else:
            distances.append((idx, float('inf')))

    if not distances:
        return False

    distances.sort(key=lambda x: x[1], reverse=True)
    farthest_idx = distances[0][0]

    logging.debug('Resolving conflict %s: reassigning POI at index %d (distance: %.1f m)',
                  group_key, farthest_idx, distances[0][1])

    # Clear every OSM-derived field online_poi_matching.py set while this row still
    # held the (now-revoked) match, not just osm_id/osm_node - otherwise the row is
    # exported as "new" (osm_id is None, so file_output.py gives it a negative
    # placeholder id) while still carrying a real osm_version/osm_timestamp/live tag
    # payload from the OSM element it no longer matches, which looks like - and is
    # a leftover of - an already-existing element. See STAGE 7 in create_db.py for
    # the matching set of columns matching starts from.
    for col in ('osm_id', 'osm_node', 'osm_version', 'osm_changeset', 'osm_timestamp',
                'osm_live_tags', 'osm_nodes'):
        if col in data.columns:
            data.at[farthest_idx, col] = None
    # This row's osm_id was set (poi_new=False) before the conflict was detected -
    # flip it back to True so file_output.py's fixme tag and STAGE 9 phase 2
    # (enrich_matched_pois()) both treat it as the new/unverified POI it now is,
    # not as a still-matched one.
    if 'poi_new' in data.columns:
        data.at[farthest_idx, 'poi_new'] = True
    return True


def match_conflict_resolution(data, db_session=None, max_iterations=10):
    """Resolve all OSM-element conflicts in the POI dataset.

    Iteratively finds all POIs sharing the same osm_id *and poi_type* (see
    find_osm_id_conflicts()) and reassigns conflicting POIs to None (effectively
    removing their OSM match) until no duplicates remain.

    Args:
        data (pd.DataFrame): POI data with osm_id column
        db_session: SQLAlchemy session (optional, for future enhancement)
        max_iterations (int): Maximum iterations to prevent infinite loops

    Returns:
        tuple: (data, resolution_stats dict)
    """
    data = data.copy()

    stats = {
        'initial_conflicts': 0,
        'resolved': 0,
        'iterations': 0,
        'unresolved': 0
    }

    iteration = 0
    while iteration < max_iterations:
        conflicts = find_osm_id_conflicts(data)
        if not conflicts:
            logging.info('All OSM ID conflicts resolved in %d iterations', iteration)
            break

        if iteration == 0:
            stats['initial_conflicts'] = len(conflicts)

        resolved_count = 0
        for group_key, conflict_indices in conflicts.items():
            if resolve_conflict(data, group_key, conflict_indices, db_session):
                resolved_count += 1

        stats['resolved'] += resolved_count
        iteration += 1

        if resolved_count == 0:
            logging.warning('No conflicts resolved in iteration %d - stopping', iteration)
            break

    final_conflicts = find_osm_id_conflicts(data)
    stats['iterations'] = iteration
    stats['unresolved'] = len(final_conflicts)

    if final_conflicts:
        logging.warning('Remaining %d OSM ID conflicts after %d iterations',
                       len(final_conflicts), iteration)

    return data, stats
