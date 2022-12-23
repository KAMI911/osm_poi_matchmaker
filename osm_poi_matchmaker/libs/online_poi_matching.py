# -*- coding: utf-8 -*-

__author__ = 'kami911'

try:
    import logging
    import sys
    import math
    import pandas as pd
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osmapi import OsmApi
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.dao.data_structure import OSM_object_type, POI_OSM_cache
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao.data_handlers import get_or_create_cache
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

RETRY = 3


def online_poi_matching(args):
    data, comm_data = args
    try:
        db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                                  config.get_database_writer_password(),
                                                  config.get_database_writer_host(),
                                                  config.get_database_writer_port(),
                                                  config.get_database_poi_database()))
        pgsql_pool = db.pool
        session_factory = sessionmaker(pgsql_pool)
        Session = scoped_session(session_factory)
        session = Session()
        osm_live_query = OsmApi()
        for i, row in data.iterrows():
        # for i, row in data[data['poi_code'].str.contains('ping')].iterrows():
            logging.info("starting")
            try:
                # Try to search OSM POI with same type, and name contains poi_search_name within the specified distance
                osm_query = db.query_osm_shop_poi_gpd(row.get('poi_lon'), row.get('poi_lat'),
                                                      comm_data.loc[comm_data['pc_id'] == row.get('poi_common_id')][
                                                          'poi_type'].values[0], row.get('poi_search_name'),
                                                      row.get('poi_search_avoid_name'), row.get('poi_name'),
                                                      row.get('additional_ref_name'),
                                                      row.get('poi_addr_street'), row.get('poi_addr_housenumber'),
                                                      row.get('poi_conscriptionnumber'), row.get('poi_city'),
                                                      row.get('osm_search_distance_perfect'),
                                                      row.get('osm_search_distance_safe'),
                                                      row.get('osm_search_distance_unsafe'))
                # Enrich our data with OSM database POI metadata
                if osm_query is not None:
                    changed_from_osm = False
                    # This is not a new POI, already found in OSM database
                    row['poi_new'] = False
                    data.at[i, 'poi_new'] = False
                    # Collect additional OSM metadata. Note: this needs style change during osm2pgsql
                    osm_id = osm_query['osm_id'].values[0] if osm_query.get('osm_id') is not None else None
                    osm_node = osm_query.get('node').values[0] if osm_query.get('node') is not None else None
                    # Set OSM POI coordinates for all kind of geom
                    lat = osm_query.get('lat').values[0]
                    lon = osm_query.get('lon').values[0]
                    if data.at[i, 'poi_lat'] != lat and data.at[i, 'poi_lon'] != lon:
                        logging.info('Using new coordinates %s %s instead of %s %s.',
                                     lat, lon, data.at[i, 'poi_lat'], data.at[i, 'poi_lon'])
                        data.at[i, 'poi_lat'] = lat
                        data.at[i, 'poi_lon'] = lon
                    if osm_node == 'node':
                        osm_node = OSM_object_type.node
                    elif osm_node == 'way':
                        osm_node = OSM_object_type.way
                    elif osm_node == 'relation':
                        osm_node = OSM_object_type.relation
                    else:
                        logging.warning('Illegal state: %s', osm_query['node'].values[0])
                    data.at[i, 'osm_id'] = osm_id
                    data.at[i, 'osm_node'] = osm_node
                    # Refine postcode
                    if row.get('preserve_original_post_code') is not True:
                        # Current OSM postcode based on lat,long query.
                        postcode = query_postcode_osm_external(config.get_geo_prefer_osm_postcode(), session, lon, lat,
                                                               row.get('poi_postcode'))
                        force_postcode_change = False  # TODO: Has to be a setting in app.conf
                        if force_postcode_change is True:
                            # Force to use datasource postcode
                            if postcode != row.get('poi_postcode'):
                                logging.info('Changing postcode from %s to %s.', row.get('poi_postcode'), postcode)
                                data.at[i, 'poi_postcode'] = postcode
                        else:
                            # Try to use smart method for postcode check
                            ch_postcode = smart_postcode_check(row, osm_query, postcode)
                            if ch_postcode is not None:
                                data.at[i, 'poi_postcode'] = ch_postcode
                    else:
                        logging.info('Preserving original postcode %s', row.get('poi_postcode'))
                    # Overwrite housenumber import data with OSM truth
                    if osm_query['addr:housenumber'].values[0] is not None and \
                       osm_query['addr:housenumber'].values[0] != '' and \
                       osm_query['addr:housenumber'].values[0] != row.get('poi_addr_housenumber'):
                        data.at[i, 'poi_addr_housenumber'] = osm_query['addr:housenumber'].values[0]
                        changed_from_osm = True
                    # Overwrite city import data with OSM truth
                    if osm_query['addr:city'].values[0] is not None and osm_query['addr:city'].values[0] != '' and \
                       osm_query['addr:city'].values[0] != row.get('poi_city'):
                        data.at[i, 'poi_city'] = osm_query['addr:city'].values[0]
                        changed_from_osm = True
                    # Overwrite street import data with OSM truth
                    if osm_query['addr:street'].values[0] is not None and osm_query['addr:street'].values[0] != '' and \
                       osm_query['addr:street'].values[0] != row.get('poi_addr_street'):
                        data.at[i, 'poi_addr_street'] = osm_query[ 'addr:street'].values[0]
                        changed_from_osm = True
                    # Overwrite conscription number import data with OSM truth
                    if osm_query['addr:conscriptionnumber'].values[0] is not None and \
                       osm_query['addr:conscriptionnumber'].values[0] != '' and \
                       osm_query['addr:conscriptionnumber'].values[0] != row.get('poi_conscriptionnumber'):
                        data.at[i, 'poi_conscriptionnumber'] = osm_query['addr:conscriptionnumber'].values[0]
                        changed_from_osm = True
                    data.at[i, 'osm_version'] = osm_query['osm_version'].values[0] \
                        if osm_query['osm_version'] is not None else None
                    data.at[i, 'osm_changeset'] = osm_query['osm_changeset'].values[0] \
                        if osm_query['osm_changeset'] is not None else None
                    if osm_query['osm_timestamp'] is not None:
                        osm_query['osm_timestamp'] = \
                            data.at[i, 'osm_timestamp'] = pd.to_datetime(str((osm_query['osm_timestamp'].values[0])))
                    else:
                        osm_query['osm_timestamp'] = None
                    data.at[i, 'poi_distance'] = osm_query.get('distance').values[0] if osm_query.get('distance') is not None else None
                    # For OSM way also query node points
                    if osm_node == OSM_object_type.way:
                        logging.info('This is an OSM way looking for id %s nodes.', osm_id)
                        # Add list of nodes to the dataframe
                        nodes = db.query_ways_nodes(osm_id)
                        data.at[i, 'osm_nodes'] = nodes
                    elif osm_node == OSM_object_type.relation:
                        logging.info('This is an OSM relation looking for id %s nodes.', osm_id)
                        # Add list of relation nodes to the dataframe
                        nodes = db.query_relation_nodes(osm_id)
                        data.at[i, 'osm_nodes'] = nodes
                    if changed_from_osm == False:
                        logging.info('Old %s (not %s) type: %s POI within %s m: %s %s, %s %s (%s)',
                                 data.at[i, 'poi_search_name'], data.at[i, 'poi_search_avoid_name'], 
                                 data.at[i, 'poi_type'], data.at[i, 'poi_distance'],
                                 data.at[i, 'poi_postcode'], data.at[i, 'poi_city'], data.at[i, 'poi_addr_street'],
                                 data.at[i, 'poi_addr_housenumber'], data.at[i, 'poi_conscriptionnumber'])
                    else:
                        logging.info('Old changed %s (not %s) type: %s POI within %s m: %s %s, %s %s (%s) was: %s %s, %s %s (%s)',
                                 data.at[i, 'poi_search_name'], data.at[i, 'poi_search_avoid_name'],
                                 data.at[i, 'poi_type'], data.at[i, 'poi_distance'],
                                 data.at[i, 'poi_postcode'], data.at[i, 'poi_city'], data.at[i, 'poi_addr_street'],
                                 data.at[i, 'poi_addr_housenumber'], data.at[i, 'poi_conscriptionnumber'],
                                 row.get('poi_postcode'), row.get('poi_city'), row.get('poi_addr_street'),
                                 row.get('poi_addr_housenumber'), row.get('poi_conscriptionnumber'))
                    try:
                        # Download OSM POI way live tags
                        if osm_node == OSM_object_type.way:
                            for rtc in range(0, RETRY):
                                logging.info('Downloading OSM live tags to this way: %s.', osm_id)
                                cached_way = db.query_from_cache(osm_id, osm_node)
                                if cached_way is None:
                                    live_tags_container = osm_live_query.WayGet(osm_id)
                                    if live_tags_container is not None:
                                        data.at[i, 'osm_live_tags'] = live_tags_container.get('tag')
                                        cache_row = {'osm_id': int(osm_id),
                                                     'osm_live_tags': live_tags_container.get('tag'),
                                                     'osm_version': live_tags_container.get('version'),
                                                     'osm_user': live_tags_container.get('user'),
                                                     'osm_user_id': live_tags_container.get('uid'),
                                                     'osm_changeset': live_tags_container.get('changeset'),
                                                     'osm_timestamp': live_tags_container.get('timestamp'),
                                                     'osm_object_type': osm_node,
                                                     'osm_lat': None,
                                                     'osm_lon': None,
                                                     'osm_nodes': live_tags_container.get('nd')}
                                        get_or_create_cache(session, POI_OSM_cache, **cache_row)
                                        # Downloading referenced nodes of the way
                                        for way_nodes in live_tags_container['nd']:
                                            logging.debug('Getting node %s belongs to way %s', way_nodes, osm_id)
                                            live_tags_node = osm_live_query.NodeGet(way_nodes)
                                            cache_row = {'osm_id': int(way_nodes),
                                                         'osm_live_tags': live_tags_node.get('tag'),
                                                         'osm_version': live_tags_node.get('version'),
                                                         'osm_user': live_tags_node.get('user'),
                                                         'osm_user_id': live_tags_node.get('uid'),
                                                         'osm_changeset': live_tags_node.get('changeset'),
                                                         'osm_timestamp': live_tags_node.get('timestamp'),
                                                         'osm_object_type': OSM_object_type.node,
                                                         'osm_lat': live_tags_node.get('lat'),
                                                         'osm_lon': live_tags_node.get('lon'),
                                                         'osm_nodes': None}
                                            get_or_create_cache(session, POI_OSM_cache, **cache_row)
                                        break
                                    else:
                                        logging.warning('Download of external data for way has failed.')
                                else:
                                    data.at[i, 'osm_live_tags'] = cached_way.get('osm_live_tags')
                                    break
                            session.commit()
                        # Download OSM POI node live tags
                        elif osm_node == OSM_object_type.node:
                            for rtc in range(0, RETRY):
                                logging.info('Downloading OSM live tags to this node: %s.', osm_id)
                                cached_node = db.query_from_cache(osm_id, osm_node)
                                if cached_node is None:
                                    live_tags_container = osm_live_query.NodeGet(osm_id)
                                    if live_tags_container is not None:
                                        data.at[i, 'osm_live_tags'] = live_tags_container.get('tag')
                                        cache_row = {'osm_id': int(osm_id),
                                                     'osm_live_tags': live_tags_container.get('tag'),
                                                     'osm_version': live_tags_container.get('version'),
                                                     'osm_user': live_tags_container.get('user'),
                                                     'osm_user_id': live_tags_container.get('uid'),
                                                     'osm_changeset': live_tags_container.get('changeset'),
                                                     'osm_timestamp': live_tags_container.get('timestamp'),
                                                     'osm_object_type': osm_node,
                                                     'osm_lat': live_tags_container.get('lat'),
                                                     'osm_lon': live_tags_container.get('lon'),
                                                     'osm_nodes': None}
                                        get_or_create_cache(session, POI_OSM_cache, **cache_row)
                                        break
                                    else:
                                        logging.warning('Download of external data for node has failed.')
                                else:
                                    data.at[i, 'osm_live_tags'] = cached_node.get('osm_live_tags')
                                    break
                            session.commit()
                        elif osm_node == OSM_object_type.relation:
                            for rtc in range(0, RETRY):
                                logging.info('Downloading OSM live tags to this relation: %s.', osm_id)
                                live_tags_container = osm_live_query.RelationGet(abs(osm_id))
                                if live_tags_container is not None:
                                    data.at[i, 'osm_live_tags'] = live_tags_container.get('tag')
                                    break
                                else:
                                    logging.warning('Download of external data for relation has failed.')
                            session.commit()
                        else:
                            logging.warning('Invalid state for live tags.')

                    except Exception as e:
                        logging.warning('There was an error during OSM request: %s.', e)
                        logging.exception('Exception occurred')
                        if cached_node is not None:
                            logging.warning('Live tag is: {}'.format(cached_node.get('osm_live_tags')))
                        else:
                            logging.warning('Live tag is None (from cached_node)')
                # This is a new POI
                else:
                    # This is a new POI - will add fix me tag to the new items.
                    data.at[i, 'poi_new'] = True
                    # Get the first character of then name of POI and generate a floating number between 0 and 1
                    # for a PostGIS function: https://postgis.net/docs/ST_LineInterpolatePoint.html
                    # If there is more than one POI in a building this will try to do a different location and
                    # not only on center or not only on edge
                    if row.get('poi_name') is not None:
                        ib = row.get('poi_name')
                    elif row.get('poi_common_name') is not None:
                        ib = row.get('poi_common_name')
                    if ib is not None:
                        ibp = 1 - (((ord(ib[0]) // 16) + 1) / 17)
                    else:
                        ibp = 0.50
                    # Refine postcode
                    osm_bulding_q = db.query_osm_building_poi_gpd(row.get('poi_lon'), row.get('poi_lat'),
                                                                  row.get('poi_city'), row.get('poi_postcode'),
                                                                  row.get('poi_addr_street'),
                                                                  row.get('poi_addr_housenumber'),
                                                                  in_building_percentage=ibp)
                    if osm_bulding_q is not None:
                        logging.info('Relocating POI coordinates to the building with same address: %s %s, %s %s',
                                     row.get('poi_lat'), row.get('poi_lon'), osm_bulding_q.get('lat')[0],
                                     osm_bulding_q.get('lon')[0]),
                        row['poi_lat'], row['poi_lon'] = osm_bulding_q.get('lat')[0], osm_bulding_q.get('lon')[0]
                    else:
                        logging.info(
                            'The POI is already in its building or there is no building match. \
                            Keeping POI coordinates as is as.')
                    if row['preserve_original_post_code'] is not True:
                        postcode = query_postcode_osm_external(config.get_geo_prefer_osm_postcode(), session,
                                                               data.at[i, 'poi_lon'], data.at[i, 'poi_lat'],
                                                               row.get('poi_postcode'))
                        if postcode != row.get('poi_postcode'):
                            logging.info('Changing postcode from %s to %s.', row.get('poi_postcode'), postcode)
                            data.at[i, 'poi_postcode'] = postcode
                    else:
                        logging.info('Preserving original postcode %s', row.get('poi_postcode'))
                    logging.info('New %s (not %s) type: %s POI: %s %s, %s %s (%s)', row.get('poi_search_name'),
                                 row.get('poi_search_avoid_name'), row.get('poi_type'), row.get('poi_postcode'),
                                 row.get('poi_city'), row.get('poi_addr_street'),
                                 row.get('poi_addr_housenumber'), row.get('poi_conscriptionnumber'))
            except Exception as e:
                logging.error(e)
                logging.error(row)
                logging.exception('Exception occurred')

        session.commit()
        logging.info("bye bye")
        return data
    except Exception as e:
        logging.error(e)
        logging.exception('Exception occurred')


def smart_postcode_check(curr_data, osm_data, osm_query_postcode):
    """
    Enhancement for the former problem: addr:postcode was changed without
    changing any other parts of address. Issue #78

    When address or conscription number change or postcode is empty.
    """
    # Change postcode when there is no postcode in OSM or the address was changed
    current_postcode = curr_data.get('poi_postcode')
    osm_db_postcode = osm_data.iloc[0, osm_data.columns.get_loc('addr:postcode')]
    if curr_data.get('poi_addr_housenumber') != osm_data.iloc[0, osm_data.columns.get_loc('addr:housenumber')] or \
       curr_data.get('poi_addr_street') != osm_data.iloc[0, osm_data.columns.get_loc('addr:street')] or \
       curr_data.get('poi_city') != osm_data.iloc[0, osm_data.columns.get_loc('addr:city')] or \
       curr_data.get('poi_addr_conscriptionnumber') != osm_data.iloc[0, osm_data.columns.get_loc('addr:conscriptionnumber')]:
        logging.debug('Address has changed via data provider so use calculated postcode if possible.')
        postcode = ordered_postcode_check([osm_query_postcode, osm_db_postcode, current_postcode])
        if postcode is None or postcode == '0':
            return None
        if postcode == osm_db_postcode:
            logging.info('The postcode is %s.', postcode)
        else:
            logging.info('Changing postcode from %s to %s.', osm_db_postcode, postcode)
        return postcode
    else:
        logging.debug('Address has not changed via data provider so use its postcode if possible.')
        postcode = ordered_postcode_check([osm_db_postcode, osm_query_postcode, current_postcode])
        if postcode is None or postcode == '0':
            return None
        if postcode == osm_db_postcode:
            logging.info('The postcode is %s.', postcode)
        else:
            logging.info('Changing postcode from %s to %s.', osm_db_postcode, postcode)
        return postcode


def ordered_postcode_check(postcode_list) -> str:
    for postcode in postcode_list:
        if postcode is not None and postcode != 0 and postcode != '0':
            return str(postcode)
    return None