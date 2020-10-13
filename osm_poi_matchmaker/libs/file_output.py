# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import math
    import os
    import datetime
    from urllib.parse import quote
    from osm_poi_matchmaker.dao.data_structure import OSM_object_type
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.address import clean_url
    from osm_poi_matchmaker.libs.osm import relationer, timestamp_now
    from osm_poi_matchmaker.libs.compare_strings import compare_strings
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osm_poi_matchmaker.dao.poi_base import POIBase
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.error(traceback.print_exc())
    sys.exit(128)

POI_TAGS = {'poi_name': 'name', 'poi_city': 'addr:city', 'poi_postcode': 'addr:postcode',
            'poi_addr_street': 'addr:street', 'poi_addr_housenumber': 'addr:housenumber',
            'poi_conscriptionnumber': 'addr:conscriptionnumber', 'poi_branch': 'branch', 'poi_email': 'email'}

POI_EV_TAGS = {'poi_capacity': 'capacity',
               'poi_socket_chademo': 'socket:chademo', 'poi_socket_chademo_output': 'socket:chademo:output',
               'poi_socket_type2_combo': 'socket:type2_combo', 'poi_socket_type2_combo_output': 'socket:type2_combo:output',
               'poi_socket_type2_cable': 'socket:type2_cable', 'poi_socket_type2_cable_output': 'socket:type2_cable:output',
               'poi_socket_type2': 'socket:type2', 'poi_socket_type2_output': 'socket:type2:output',
               'poi_manufacturer': 'manufacturer', 'poi_model': 'model'}


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
        logging.info('Saving {%s to file: %s', message, file)
        res = data.to_csv(os.path.join(path, file))
        logging.info('The %s was sucessfully saved', file)
    except Exception as err:
        logging.error(err)
        logging.error(traceback.print_exc())


def generate_osm_xml(df, session=None):
    db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                              config.get_database_writer_password(),
                                              config.get_database_writer_host(),
                                              config.get_database_writer_port(),
                                              config.get_database_poi_database()))
    pgsql_pool = db.pool
    session_factory = sessionmaker(pgsql_pool)
    Session = scoped_session(session_factory)
    session = Session()
    from lxml import etree
    import lxml
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    default_osm_id = -1
    current_osm_id = default_osm_id
    added_nodes = []
    try:
        for index, row in df.iterrows():
            current_osm_id = default_osm_id if row.get('osm_id') is None else row.get('osm_id')
            osm_timestamp = timestamp_now() if row.get('osm_timestamp') is None else row.get('osm_timestamp')
            osm_version = '99999' if row.get('osm_version') is None else row.get('osm_version')
            if row.get('osm_node') is None or row.get('osm_node') == OSM_object_type.node:
                main_data = etree.SubElement(osm_xml_data, 'node', action='modify', id=str(current_osm_id),
                    lat='{}'.format(row.get('poi_lat')), lon='{}'.format(row.get('poi_lon')),
                    user='{}'.format('osm_poi_matchmaker'), timestamp='{}'.format(osm_timestamp),
                    uid='{}'.format('8635934'),
                    version='{}'.format(osm_version))
                josm_object = 'n{}'.format(current_osm_id)
                if current_osm_id > 0:
                    comment = etree.Comment(' OSM link: https://osm.org/node/{} '.format(str(current_osm_id)))
                    osm_xml_data.append(comment)
            elif row.get('osm_node') is not None and row.get('osm_node') == OSM_object_type.way:
                main_data = etree.SubElement(osm_xml_data, 'way', action='modify', id=str(current_osm_id),
                    user='{}'.format('osm_poi_matchmaker'), timestamp='{}'.format(osm_timestamp),
                    uid='{}'.format('8635934'),
                    version='{}'.format(osm_version))
                josm_object = 'w{}'.format(current_osm_id)
                # Add way nodes without any modification)
                try:
                    for n in row.get('osm_nodes'):
                        data = etree.SubElement(main_data, 'nd', ref=str(n))
                    if session is not None:
                        # Go through the list except the last value (which is same as the first)
                        for n in row.get('osm_nodes'):
                            # Add nodes only when it is not already added.
                            if n not in added_nodes:
                                added_nodes.append(n)
                                way_node = db.query_from_cache(n, OSM_object_type.node)
                                if way_node is not None:
                                    node_data = etree.SubElement(osm_xml_data, 'node', action='modify', id=str(n),
                                        lat='{}'.format(way_node.get('osm_lat')),
                                        lon='{}'.format(way_node.get('osm_lon')),
                                        user='{}'.format(way_node.get('osm_user')),
                                        timestamp='{:{dfmt}T{tfmt}Z}'.
                                            format(way_node.get('osm_timestamp'),dfmt='%Y-%m-%d', tfmt='%H:%M:%S'),
                                        uid='{}'.format(way_node.get('osm_user_id')),
                                        version='{}'.format(way_node.get('osm_version')))
                            osm_xml_data.append(node_data)
                except TypeError as err:
                    logging.warning('Missing nodes on this way: %s.', row.get('osm_id'))
                    logging.warning(traceback.print_exc())
                # Add node reference as comment for existing POI
                if current_osm_id > 0:
                    comment = etree.Comment(' OSM link: https://osm.org/way/{} '.format(str(current_osm_id)))
                    osm_xml_data.append(comment)
            elif row.get('osm_node') is not None and row.get('osm_node') == OSM_object_type.relation:
                main_data = etree.SubElement(osm_xml_data, 'relation', action='modify', id=str(current_osm_id),
                    user='{}'.format('osm_poi_matchmaker'), timestamp='{}'.format(osm_timestamp),
                    uid='{}'.format('8635934'),
                    version='{}'.format(osm_version))
                josm_object = 'r{}'.format(current_osm_id)
                relations = relationer(row.get('osm_nodes'))
                try:
                    for i in relations:
                        data = etree.SubElement(main_data, 'member',
                            type=i.get('type'), ref=i.get('ref'), role=i.get('role'))
                except TypeError as err:
                    logging.warning('Missing nodes on this relation: %s.', row['osm_id'])
            # Add original POI coordinates as comment
            comment = etree.Comment(' Original coordinates: {} '.format(row.get('poi_geom')))
            osm_xml_data.append(comment)
            if 'poi_distance' in row:
                if row.get('poi_distance') is not None:
                    comment = etree.Comment(' OSM <-> POI distance: {} m'.format(row.get('poi_distance')))
                else:
                    comment = etree.Comment(' OSM <-> POI distance: Non exist')
                osm_xml_data.append(comment)
            if 'poi_good' in row and 'poi_bad' in row:
                comment = etree.Comment(' Checker good: {}; bad {}'.format(row.get('poi_good'), row.get('poi_bad')))
                osm_xml_data.append(comment)
            # Using already definied OSM tags if exists
            if row.get('osm_live_tags') is not None:
                tags = row.get('osm_live_tags')
                osm_live_tags = row.get('osm_live_tags').copy()
            else:
                tags = {}
                osm_live_tags = {}
            # Adding POI common tags
            if row['poi_tags'] is not None:
                tags.update(eval(row.get('poi_tags')))
            # Save live name tags if preserve name is enabled
            try:
                if row.get('preserve_original_name') == True:
                    preserved_name = tags.get('name')
            except KeyError as err:
                logging.debug('No name tag is specified to save in original OpenStreetMap data.')
            # Overwriting with data from data providers
            for k, v in POI_TAGS.items():
                if row.get(k) is not None:
                    tags[v] = row.get(k)
            if config.get_geo_alternative_opening_hours():
                alternative_oh_tag = config.get_geo_alternative_opening_hours_tag()
                # Alternative opening_hours handling for COVID-19 code path
                if tags.get('opening_hours') is not None and tags.get('opening_hours') != '':
                    if row.get('poi_opening_hours') is not None and row.get('poi_opening_hours') != '':
                        if tags.get('opening_hours') == row.get('poi_opening_hours'):
                            tags[alternative_oh_tag] = 'same'
                        else:
                            tags[alternative_oh_tag] = row.get('poi_opening_hours')
                else:
                    if row.get('poi_opening_hours') is not None and row.get('poi_opening_hours') != '':
                        tags['opening_hours'] = row.get('poi_opening_hours')
                        tags[alternative_oh_tag] = 'same'
            else:
                # Alternative opening_hours handling for NON COVID-19 code path: just simply add opening_hours to tags
                if row.get('poi_opening_hours') is not None and row.get('poi_opening_hours') != '':
                    tags['opening_hours'] = row.get('poi_opening_hours')
            # If we got POI phone tag use it as OSM contact:phone tag
            if row.get('poi_phone') is not None and row.get('poi_phone') != '':
                tags['contact:phone'] = row.get('poi_phone')
            # If we got POI website tag use it as OSM contact:website tag
            if row.get('poi_url_base') is not None and row.get('poi_website') is not None:
                if row['poi_url_base'] in row.get('poi_website'):
                    # The POI website contains the base URL use the POI website field only
                    tags['contact:website'] = clean_url('{}'.format((row.get('poi_website'))))
                else:
                    # The POI website does not contain the base URL use the merged base URL and POI website field
                    tags['contact:website'] = clean_url('{}/{}'.format(row.get('poi_url_base'), row.get('poi_website')))
            # If only the base URL is available
            elif row.get('poi_url_base') is not None:
                tags['contact:website'] = row.get('poi_url_base')
            # Short URL for source
            if row['poi_url_base'] is not None:
                source_url = 'source:{}:date'.format(row.get('poi_url_base').split('/')[2])
            else:
                source_url = 'source:website:date'
            tags[source_url] = '{:{dfmt}}'.format(datetime.datetime.now(), dfmt='%Y-%m-%d')
            # Write back the saved name tag
            if 'preserved_name' in locals():
                tags['name'] = preserved_name
            # Rewrite old contact tags to contact:* tag form
            tags_rewrite = [ 'website', 'phone', 'email', 'facebook', 'instagram', 'youtube', 'pinterest', 'fax']
            for tr in tags_rewrite:
                if tr in tags:
                    # Never overwrite already existing contact:* tags
                    if 'contact:' + tr in tags:
                        # We already have this contact:* tag so remove the simple contact tag
                        tags.pop(tr, None)
                    else:
                        # Rewrite simple contact tag to contact:* tag
                        tags['contact:' + tr] = tags.pop(tr, None)
            if row.get('poi_description') is not None and row.get('poi_description') != '':
                tags['description'] = row.get('poi_description')
            if row.get('poi_fuel_adblue') is not None and row.get('poi_fuel_adblue') != '':
                tags['fuel:adblue'] = 'yes' if row.get('poi_fuel_adblue') == True else 'no'
            if row.get('poi_fuel_octane_100') is not None and row.get('poi_fuel_octane_100') != '':
                tags['fuel:octane_100'] = 'yes' if row.get('poi_fuel_octane_100') == True else 'no'
            if row.get('poi_fuel_octane_98') is not None and row.get('poi_fuel_octane_98') != '':
                tags['fuel:octane_98'] = 'yes' if row.get('poi_fuel_octane_98') == True else 'no'
            if row.get('poi_fuel_octane_95') is not None and row.get('poi_fuel_octane_95') != '':
                tags['fuel:octane_95'] = 'yes' if row.get('poi_fuel_octane_95') == True else 'no'
            if row.get('poi_fuel_diesel_gtl') is not None and row.get('poi_fuel_diesel_gtl') != '':
                tags['fuel:GTL_diesel'] = 'yes' if row.get('poi_fuel_diesel_gtl') == True else 'no'
            if row.get('poi_fuel_diesel') is not None and row.get('poi_fuel_diesel') != '':
                tags['fuel:diesel'] = 'yes' if row.get('poi_fuel_diesel') == True else 'no'
            if row.get('poi_fuel_lpg') is not None and row.get('poi_fuel_lpg') != '':
                tags['fuel:lpg'] = 'yes' if row.get('poi_fuel_lpg') == True else 'no'
            if row.get('poi_fuel_e85') is not None and row.get('poi_fuel_e85') != '':
                tags['fuel:e85'] = 'yes' if row.get('poi_fuel_e85') == True else 'no'
            if row.get('poi_rent_lpg_bottles') is not None and row.get('poi_rent_lpg_bottles') != '':
                tags['rent:lpg_bottles'] = 'yes' if row.get('poi_rent_lpg_bottles') == True else 'no'
            if row.get('poi_compressed_air') is not None and row.get('poi_compressed_air') != '':
                tags['compressed_air'] = 'yes' if row.get('poi_compressed_air') == True else 'no'
            if row.get('poi_restaurant') is not None and row.get('poi_restaurant') != '':
                tags['restaurant'] = 'yes' if row.get('poi_restaurant') == True else 'no'
            if row.get('poi_food') is not None and row.get('poi_food') != '':
                tags['food'] = 'yes' if row.get('poi_food') == True else 'no'
            if row.get('poi_truck') is not None and row.get('poi_truck') != '':
                tags['truck'] = 'yes' if row.get('poi_truck') == True else 'no'
            if row.get('poi_authentication_app') is not None and row.get('poi_authentication_app') != '':
                tags['authentication:app'] = 'yes' if row.get('poi_authentication_app') == True else 'no'
            if row.get('poi_authentication_membership_card') is not None and row.get('poi_authentication_membership_card') != '':
                tags['authentication:membership_card'] = 'yes' if row.get('poi_authentication_membership_card') == True else 'no'
            if row.get('poi_fee') is not None and row.get('poi_fee') != '':
                tags['fee'] = 'yes' if row.get('poi_fee') == True else 'no'
            if row.get('poi_parking_fee') is not None and row.get('poi_parking_fee') != '':
                tags['parking_fee'] = 'yes' if row.get('poi_parking_fee') == True else 'no'
            if row.get('poi_motorcar') is not None and row.get('poi_motorcar') != '':
                tags['motorcar'] = 'yes' if row.get('poi_motorcar') == True else 'no'
            for k, v in POI_EV_TAGS.items():
                if row.get(k) is not None and row.get(k) != '':
                    if isinstance(row.get(k), float):
                        if not math.isnan(row.get(k)):
                            tags[v] = int(row.get(k))
                    else:
                        tags[v] = row.get(k)
            # This is a new POI - will add fix me tag to the new items.
            if row.get('poi_new') is not None and row.get('poi_new') == True:
                tags['fixme'] = 'verify import'
            # Remove unwanted addr:country from file output as we discussed in Issue #33
            tags.pop('addr:country', None)
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
                        v = v.replace('-', '\-').replace('\n', '')
                    w = osm_live_tags[k]
                except KeyError:
                    comment += "{:32} N\t\t'{}'\n".format(k, v)
                else:
                    if isinstance(w, str):
                        w = w.replace('-', '\-').replace('\n', '')
                    comment += "{:32} {}\t\t'{}'\t\t\t'{}'\n".format(k, compare_strings(v,w), v, w)
            comment = etree.Comment(comment)
            osm_xml_data.append(comment)
            # URL encode link and '--' in comment
            josm_link = quote(josm_link)
            josm_link = josm_link.replace('--', '%2D%2D')
            comment = etree.Comment(' JOSM magic link: {}?new_layer=false&objects={}&addtags={} '.format('http://localhost:8111/load_object', josm_object, josm_link))
            osm_xml_data.append(comment)
            osm_xml_data.append(main_data)
            # Next deafult OSM id is one more less for non existing objects
            default_osm_id -= 1
    except ValueError as e:
        logging.error(e)
        logging.error(comment)
        logging.error(traceback.print_exc())
    except Exception as e:
        logging.error(e)
        logging.error(traceback.print_exc())
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")
