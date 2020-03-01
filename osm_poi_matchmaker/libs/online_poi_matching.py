# -*- coding: utf-8 -*-

__author__ = 'kami911'

try:
    import traceback
    import logging
    import sys
    import pandas as pd
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osmapi import OsmApi
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.dao.data_structure import OSM_object_type, POI_OSM_cache
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao.data_handlers import get_or_create
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
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
        mysql_pool = db.pool
        session_factory = sessionmaker(mysql_pool)
        Session = scoped_session(session_factory)
        session = Session()
        osm_live_query = OsmApi()
        for i, row in data.iterrows():
        # for i, row in data[data['poi_code'].str.contains('tesco')].iterrows():
            common_row = comm_data.loc[comm_data['pc_id'] == row.get('poi_common_id')]
            # Try to search OSM POI with same type, and name contains poi_search_name within the specified distance
            osm_query = db.query_osm_shop_poi_gpd(row.get('poi_lon'), row.get('poi_lat'),
                common_row.get('poi_type').item(), row.get('poi_search_name'), row.get('poi_addr_street'),
                row.get('poi_addr_housenumber'), row.get('poi_addr_conscriptionnumber'), row.get('poi_addr_city'),
                row.get('osm_search_distance_perfect'), row.get('osm_search_distance_safe'),
                row.get('osm_search_distance_unsafe'))
            # Enrich our data with OSM database POI metadata
            if osm_query is not None:
                row['poi_new'] = False
                # Collect additional OSM metadata. Note: this needs style change during osm2pgsql
                osm_id = osm_query['osm_id'].values[0]
                if osm_query['node'].values[0] == 'node':
                    osm_node = OSM_object_type.node
                elif osm_query['node'].values[0] == 'way':
                    osm_node = OSM_object_type.way
                elif osm_query['node'].values[0] == 'relation':
                    osm_node = OSM_object_type.relation
                else:
                    logging.warning('Illegal state: {}'.format(osm_query['node'].values[0]))
                # Set OSM POI coordinates for the node
                if osm_node == OSM_object_type.node:
                    data.at[i, 'poi_lat'] = osm_query['lat'].values[0]
                    data.at[i, 'poi_lon'] = osm_query['lon'].values[0]
                # Refine postcode
                postcode = query_postcode_osm_external(config.get_geo_prefer_osm_postcode(), session,
                                                       row.get('poi_lon'), row.get('poi_lat'), row.get('poi_postcode'))
                if postcode != row.get('poi_postcode'):
                    logging.info('Changing postcode from {} to {}.'.format(row.get('poi_postcode'), postcode))
                    data.at[i, 'poi_postcode'] = postcode
                data.at[i, 'osm_id'] = osm_id
                data.at[i, 'osm_node'] = osm_node
                data.at[i, 'osm_version'] = osm_query['osm_version'].values[0]
                data.at[i, 'osm_changeset'] = osm_query['osm_changeset'].values[0]
                osm_timestamp = pd.to_datetime(str((osm_query['osm_timestamp'].values[0])))
                data.at[i, 'osm_timestamp'] = '{:{dfmt}T{tfmt}Z}'.format(osm_timestamp, dfmt='%Y-%m-%d', tfmt='%H:%M:%S')
                data.loc[[i], 'poi_distance'] = osm_query['distance'].values[0]
                # For OSM way also query node points
                if osm_node == OSM_object_type.way:
                    logging.info('This is an OSM way looking for id {} nodes.'.format(osm_id))
                    # Add list of nodes to the dataframe
                    nodes = db.query_ways_nodes(osm_id)
                    data.at[i, 'osm_nodes'] = nodes
                elif osm_node == OSM_object_type.relation:
                    logging.info('This is an OSM relation looking for id {} nodes.'.format(osm_id))
                    # Add list of relation nodes to the dataframe
                    nodes = db.query_relation_nodes(osm_id)
                    data.at[i, 'osm_nodes'] = nodes
                try:
                    # Download OSM POI way live tags
                    if osm_node == OSM_object_type.way:
                        for rtc in range(0, RETRY):
                            logging.info('Downloading OSM live tags to this way: {}.'.format(osm_id))
                            cached_way = db.query_from_cache(osm_id, osm_node)
                            if cached_way is None:
                                live_tags_container = osm_live_query.WayGet(osm_id)
                                if live_tags_container is not None:
                                    data.at[i, 'osm_live_tags'] = live_tags_container.get('tag')
                                    cache_row = {'osm_id': int(osm_id), 'osm_live_tags': str(live_tags_container.get('tag')),
                                             'osm_version': live_tags_container.get('version'),
                                             'osm_user': live_tags_container.get('user'),
                                             'osm_user_id': live_tags_container.get('uid'),
                                             'osm_changeset': live_tags_container.get('changeset'),
                                             'osm_timestamp': str(live_tags_container.get('timestamp')),
                                             'osm_object_type': osm_node,
                                             'osm_lat': None,
                                             'osm_lon': None,
                                             'osm_nodes': str(live_tags_container.get('nd'))}
                                    get_or_create(session, POI_OSM_cache, **cache_row)
                                    # Downloading referenced nodes of the way
                                    for way_nodes in live_tags_container['nd']:
                                        logging.debug('Getting node {} belongs to way {}'.format(way_nodes, osm_id))
                                        live_tags_node = osm_live_query.NodeGet(way_nodes)
                                        cache_row = {'osm_id': int(way_nodes), 'osm_live_tags': str(live_tags_node.get('tag')),
                                                     'osm_version': live_tags_node.get('version'),
                                                     'osm_user': live_tags_node.get('user'),
                                                     'osm_user_id': live_tags_node.get('uid'),
                                                     'osm_changeset': live_tags_node.get('changeset'),
                                                     'osm_timestamp': str(live_tags_node.get('timestamp')),
                                                     'osm_object_type': OSM_object_type.node,
                                                     'osm_lat': live_tags_node.get('lat'),
                                                     'osm_lon': live_tags_node.get('lon'),
                                                     'osm_nodes': None}
                                        get_or_create(session, POI_OSM_cache, **cache_row)
                                    break
                                else:
                                    logging.warning('Download of external data has failed.')
                            else:
                                data.at[i, 'osm_live_tags'] = eval(cached_way.get('osm_live_tags'))
                                break
                        session.commit()
                    # Download OSM POI node live tags
                    elif osm_node == OSM_object_type.node:
                        for rtc in range(0, RETRY):
                            logging.info('Downloading OSM live tags to this node: {}.'.format(osm_id))
                            cached_node = db.query_from_cache(osm_id, osm_node)
                            if cached_node is None:
                                live_tags_container = osm_live_query.NodeGet(osm_id)
                                if live_tags_container is not None:
                                    data.at[i, 'osm_live_tags'] = live_tags_container.get('tag')
                                    cache_row = {'osm_id': int(osm_id),
                                        'osm_live_tags': str(live_tags_container.get('tag')),
                                        'osm_version': live_tags_container.get('version'),
                                        'osm_user': live_tags_container.get('user'),
                                        'osm_user_id': live_tags_container.get('uid'),
                                        'osm_changeset': live_tags_container.get('changeset'),
                                        'osm_timestamp': str(live_tags_container.get('timestamp')),
                                        'osm_object_type': osm_node,
                                        'osm_lat': live_tags_container.get('lat'),
                                        'osm_lon': live_tags_container.get('lon'),
                                        'osm_nodes': None}
                                    get_or_create(session, POI_OSM_cache, **cache_row)
                                    break
                                else:
                                    logging.warning('Download of external data has failed.')
                            else:
                                data.at[i, 'osm_live_tags'] = eval(cached_node['osm_live_tags'])
                                break
                        session.commit()
                    elif osm_node == OSM_object_type.relation:
                        for rtc in range(0, RETRY):
                            logging.info('Downloading OSM live tags to this relation: {}.'.format(osm_id))
                            live_tags_container = osm_live_query.RelationGet(abs(osm_id))
                            if live_tags_container is not None:
                                data.at[i, 'osm_live_tags'] = live_tags_container.get('tag')
                                break
                            else:
                                logging.warning('Download of external data has failed.')
                        session.commit()
                    else:
                        logging.warning('Invalid state for live tags.')

                except Exception as err:
                    logging.warning('There was an error during OSM request: {}.'.format(err))
                    logging.warning(traceback.print_exc())
            # This is a new POI
            else:
                # This is a new POI - will add fix me tag to the new items.
                data.at[i, 'poi_new'] = True
                # Get the first character of then name of POI and generate a floating number between 0 and 1
                # for a PostGIS function: https://postgis.net/docs/ST_LineInterpolatePoint.html
                # If there is more than one POI in a building this will try to do a different location and
                # not only on center or not only on edge
                ib = row.get('poi_name')
                if ib is not None:
                    ibp = 1 - (((ord(ib[0]) // 16) + 1) / 17)
                else:
                    ibp = 0.50
                logging.info('New {} type: {} POI: {} {}, {} {}'.format(row.get('poi_search_name'), row.get('poi_type'),
                    row.get('poi_postcode'), row.get('poi_city'), row.get('poi_addr_street'),
                    row.get('poi_addr_housenumber')))
                # Refine postcode
                data.at[i, 'poi_postcode'] = query_postcode_osm_external(config.get_geo_prefer_osm_postcode(), session,
                    row.get('poi_lon'), row.get('poi_lat'), row.get('poi_postcode'))
                osm_bulding_q = db.query_osm_building_poi_gpd(row.get('poi_lon'), row.get('poi_lat'),
                    row.get('poi_city'), row.get('poi_postcode'), row.get('poi_addr_street'),
                    row.get('poi_addr_housenumber'), in_building_percentage=ibp)
                if osm_bulding_q is not None:
                    logging.info('Relocating POI coordinates to the building with same address: {} {}, {} {}'.format(
                        row.get('poi_lat'), row.get('poi_lon'), osm_bulding_q.get('lat')[0], osm_bulding_q.get('lon')[0])),
                    row['poi_lat'], row['poi_lon'] = osm_bulding_q.get('lat')[0], osm_bulding_q.get('lon')[0]
                else:
                    logging.info('The POI is already in its building or there is no building match. Keeping POI coordinates as is as.')
        session.commit()
        return data
    except Exception as err:
        logging.error(err)
        logging.error(traceback.print_exc())
