# -*- coding: utf-8 -*-

try:
    import traceback
    import math
    import logging
    import os
    import datetime
    from urllib.parse import quote
    from dao.data_structure import OSM_object_type
    from utils import config
    from libs.address import clean_url
    from libs.osm import relationer, timestamp_now
    from libs.compare_strings import compare_strings
    from sqlalchemy.orm import scoped_session, sessionmaker
    from dao.poi_base import POIBase
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_TAGS = {'poi_name': 'name', 'poi_city': 'addr:city', 'poi_postcode': 'addr:postcode',
            'poi_addr_street': 'addr:street', 'poi_addr_housenumber': 'addr:housenumber',
            'poi_conscriptionnumber': 'addr:conscriptionnumber', 'poi_branch': 'branch', 'poi_email': 'email',
            'poi_opening_hours': 'opening_hours'}


def ascii_numcoder(text):
    output = ''
    for i in text:
        if i in range(0, 10, 1):
            output += i
        else:
            output += str(ord(i))
    return output


def save_csv_file(path, file, data, message):
    try:
        # Save file to CSV file
        logging.info('Saving {0} to file: {1}'.format(message, file))
        res = data.to_csv(os.path.join(path, file))
        logging.info('The {0} was sucessfully saved'.format(file))
    except Exception as err:
        logging.error(err)
        logging.error(traceback.print_exc())


def generate_osm_xml(df, session=None):
    db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                              config.get_database_writer_password(),
                                              config.get_database_writer_host(),
                                              config.get_database_writer_port(),
                                              config.get_database_poi_database()))
    mysql_pool = db.pool
    session_factory = sessionmaker(mysql_pool)
    Session = scoped_session(session_factory)
    session = Session()
    from lxml import etree
    import lxml
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    id = -1
    current_id = id
    added_nodes = []
    try:
        for index, row in df.iterrows():
            current_id = id if row['osm_id'] is None else row['osm_id']
            osm_timestamp = timestamp_now() if row['osm_timestamp'] is None else row['osm_timestamp']
            osm_version = '99999' if row['osm_version'] is None else row['osm_version']
            if row['osm_node'] is None or row['osm_node'] == OSM_object_type.node:
                main_data = etree.SubElement(osm_xml_data, 'node', action='modify', id=str(current_id),
                                             lat='{}'.format(row['poi_lat']), lon='{}'.format(row['poi_lon']),
                                             user='{}'.format('osm_poi_matchmaker'), timestamp='{}'.format(osm_timestamp),
                                             uid='{}'.format('8635934'),
                                             version='{}'.format(osm_version))
                josm_object = 'n{}'.format(current_id)
                if current_id > 0:
                    comment = etree.Comment(' OSM link: https://osm.org/node/{} '.format(str(current_id)))
                    osm_xml_data.append(comment)
            elif row['osm_node'] is not None and row['osm_node'] == OSM_object_type.way:
                main_data = etree.SubElement(osm_xml_data, 'way', action='modify', id=str(current_id),
                                             user='{}'.format('osm_poi_matchmaker'), timestamp='{}'.format(osm_timestamp),
                                             uid='{}'.format('8635934'),
                                             version='{}'.format(osm_version))
                josm_object = 'w{}'.format(current_id)
                # Add way nodes without any modification)
                try:
                    for n in row['osm_nodes']:
                        data = etree.SubElement(main_data, 'nd', ref=str(n))
                    if session is not None:
                        # Go through the list except the last value (which is same as the first)
                        for n in row['osm_nodes']:
                            # Add nodes only when it is not already added.
                            if n not in added_nodes:
                                added_nodes.append(n)
                                way_node = db.query_from_cache(n, OSM_object_type.node)
                                node_data = etree.SubElement(osm_xml_data, 'node', action='modify', id=str(n),
                                                         lat='{}'.format(way_node['osm_lat']),
                                                         lon='{}'.format(way_node['osm_lon']),
                                                         user='{}'.format(way_node['osm_user']),
                                                         timestamp='{:{dfmt}T{tfmt}Z}'.format(way_node['osm_timestamp'], dfmt='%Y-%m-%d', tfmt='%H:%M:%S'),
                                                         uid='{}'.format(way_node['osm_user_id']),
                                                         version='{}'.format(way_node['osm_version']))
                            osm_xml_data.append(node_data)
                except TypeError as err:
                    logging.warning('Missing nodes on this way: {}.'.format(row['osm_id']))
                    logging.warning(traceback.print_exc())
                # Add node reference as comment for existing POI
                if current_id > 0:
                    comment = etree.Comment(' OSM link: https://osm.org/way/{} '.format(str(current_id)))
                    osm_xml_data.append(comment)
            elif row['osm_node'] is not None and row['osm_node'] == OSM_object_type.relation:
                main_data = etree.SubElement(osm_xml_data, 'relation', action='modify', id=str(current_id),
                                             user='{}'.format('osm_poi_matchmaker'), timestamp='{}'.format(osm_timestamp),
                                             uid='{}'.format('8635934'),
                                             version='{}'.format(osm_version))
                josm_object = 'r{}'.format(current_id)
                relations = relationer(row['osm_nodes'])
                try:
                    for i in relations:
                        data = etree.SubElement(main_data, 'member', type=i['type'], ref=i['ref'], role=i['role'])
                except TypeError as err:
                    logging.warning('Missing nodes on this relation: {}.'.format(row['osm_id']))
            # Add original POI coordinates as comment
            comment = etree.Comment(' Original coordinates: {} '.format(row['poi_geom']))
            osm_xml_data.append(comment)
            if 'poi_distance' in row:
                comment = etree.Comment(' OSM <-> POI distance: {} m'.format(row['poi_distance']))
                osm_xml_data.append(comment)
            if 'poi_good' in row and 'poi_bad' in row:
                comment = etree.Comment(' Checker good: {}; bad {}'.format(row['poi_good'], row['poi_bad']))
                osm_xml_data.append(comment)
            # Using already definied OSM tags if exists
            if row['osm_live_tags'] is not None:
                tags = row['osm_live_tags']
                osm_live_tags = row['osm_live_tags'].copy()
            else:
                tags = {}
                osm_live_tags = {}
            # Adding POI common tags
            if row['poi_tags'] is not None:
                tags.update(eval(row['poi_tags']))
            # Save live name tags if preserve name is enabled
            try:
                if row['preserve_original_name'] == True:
                    preserved_name = tags['name']
            except KeyError as err:
                logging.debug('No name tag is specified to save in original OpenStreetMap data.')
            # Overwriting with data from data providers
            for k, v in POI_TAGS.items():
                if row[k] is not None:
                    tags[v] = row[k]
            # If we got POI phone tag use it as OSM contact:phone tag
            if row['poi_phone'] is not None and row['poi_phone'] != '':
                tags['contact:phone'] = row['poi_phone']
            # If we got POI website tag use it as OSM contact:website tag
            if row['poi_url_base'] is not None and row['poi_website'] is not None:
                if row['poi_url_base'] in row['poi_website']:
                    # The POI website contains the base URL use the POI website field only
                    tags['contact:website'] = clean_url('{}'.format((row['poi_website'])))
                else:
                    # The POI website does not contain the base URL use the merged base URL and POI website field
                    tags['contact:website'] = clean_url('{}/{}'.format(row['poi_url_base'], row['poi_website']))
            # If only the base URL is available
            elif row['poi_url_base'] is not None:
                tags['contact:website'] = row['poi_url_base']
            # Short URL for source
            if row['poi_url_base'] is not None:
                source_url = 'source:{}:date'.format(row['poi_url_base'].split('/')[2])
            else:
                source_url = 'source:website:date'
            tags[source_url] = '{:{dfmt}}'.format(datetime.datetime.now(), dfmt='%Y-%m-%d')
            # Write back the saved name tag
            if 'preserved_name' in locals():
                tags['name'] = preserved_name
            # tags['import'] = 'osm_poi_matchmaker'
            # Rendering tags to the XML file and JOSM magic link
            josm_link = ''
            comment = ('\nKey\t\t\t\tStatus\t\tNew value\t\tOSM value\n')
            for k, v in sorted(tags.items()):
                xml_tags = etree.SubElement(main_data, 'tag', k=k, v='{}'.format(v))
                josm_link = '{}|{}={}'.format(josm_link, k, v)
                # Add original POI tags as comment
                try:
                    if isinstance(v, str):
                        v = v.replace('--', '\-\-').replace('\n', '')
                    w = osm_live_tags[k]
                except KeyError:
                    comment += "{:32} N\t\t'{}'\n".format(k, v)
                else:
                    if isinstance(w, str):
                        w = w.replace('--', '\-\-').replace('\n', '')
                    comment += "{:32} {}\t\t'{}'\t\t\t'{}'\n".format(k, compare_strings(v,w), v, w)
            comment = etree.Comment(comment)
            # URL encode link and '--' in comment
            josm_link = quote(josm_link)
            josm_link = josm_link.replace('--', '%2D%2D')
            osm_xml_data.append(comment)
            comment = etree.Comment(' JOSM magic link: {}?new_layer=false&objects={}&addtags={} '.format('http://localhost:8111/load_object', josm_object, josm_link))
            osm_xml_data.append(comment)
            osm_xml_data.append(main_data)
            id -= 1
    except Exception as err:
        logging.error(err)
        logging.error(traceback.print_exc())
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")
