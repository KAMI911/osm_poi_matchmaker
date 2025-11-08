# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import math
    import os
    import datetime
    import traceback
    from urllib.parse import quote
    import numpy as np
    from osm_poi_matchmaker.dao.data_structure import OSM_object_type
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.address import clean_url
    from osm_poi_matchmaker.libs.file_output_helper import url_tag_generator
    from osm_poi_matchmaker.libs.osm import relationer, timestamp_now
    from osm_poi_matchmaker.libs.compare_strings import compare_strings
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from lxml import etree
    import lxml
    import re
    import json
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POI_TAGS = {'poi_common_name': 'name', 'poi_city': 'addr:city', 'poi_postcode': 'addr:postcode',
            'poi_addr_street': 'addr:street', 'poi_addr_housenumber': 'addr:housenumber',
            'poi_conscriptionnumber': 'addr:conscriptionnumber', 'poi_branch': 'branch', 'poi_email': 'email'}

# TODO: Separete adblue_pack, adblue_car and adblue_truck tags
# TODO: fuel:adblue=yes; fuel:adblue:canister=yes;fuel:adblue:motorcar=yes;fuel:adblue:hgv=yes

POI_YESNO_TAGS = {'poi_fuel_adblue': 'fuel:adblue', 'poi_fuel_octane_100': 'fuel:octane_100',
                  'poi_fuel_octane_98': 'fuel:octane_98', 'poi_fuel_octane_95': 'fuel:octane_95',
                  'poi_fuel_diesel_gtl': 'fuel:GTL_diesel', 'poi_fuel_diesel': 'fuel:diesel',
                  'poi_fuel_lpg': 'fuel:lpg', 'poi_fuel_e85': 'fuel:e85', 'poi_rent_lpg_bottles': 'rent:lpg_bottles',
                  'poi_compressed_air': 'compressed_air', 'poi_restaurant': 'restaurant', 'poi_food': 'food',
                  'poi_truck': 'truck', 'poi_authentication_app': 'authentication:app',
                  'poi_authentication_membership_card': 'authentication:membership_card', 'poi_fee': 'fee',
                  'poi_parking_fee': 'parking_fee', 'poi_motorcar': 'motorcar'}

POI_EV_TAGS = {'poi_capacity': 'capacity',
               'poi_socket_chademo': 'socket:chademo',
               'poi_socket_chademo_output': 'socket:chademo:output',
               'poi_socket_chademo_current': 'socket:chademo:current',
               'poi_socket_chademo_voltage': 'socket:chademo:voltage',

               'poi_socket_type2_combo': 'socket:type2_combo',
               'poi_socket_type2_combo_output': 'socket:type2_combo:output',
               'poi_socket_type2_combo_current': 'socket:type2_combo:current',
               'poi_socket_type2_combo_voltage': 'socket:type2_combo:voltage',

               'poi_socket_type2_cable': 'socket:type2_cable',
               'poi_socket_type2_cable_output': 'socket:type2_cable:output',
               'poi_socket_type2_cable_current': 'socket:type2_cable:current',
               'poi_socket_type2_cable_voltage': 'socket:type2_cable:voltage',
               
               'poi_socket_type2_cableless': 'socket:type2',
               'poi_socket_type2_cableless_output': 'socket:type2:output',
               'poi_socket_type2_cableless_current': 'socket:type2:current',
               'poi_socket_type2_cableless_voltage': 'socket:type2:voltage',

               'poi_manufacturer': 'manufacturer', 'poi_model': 'model'}

TESTCASE_GEN_KEYS = ('original', 'poi_postcode', 'poi_city', 'poi_addr_street', 'poi_addr_housenumber',
                     'poi_conscriptionnumber')

TIMESTAMP_FORMAT = '{:{dfmt}T{tfmt}Z}'
DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'

COMPLEX_OH_PATTERNS = [
    r'sunrise', r'sunset', r'by appointment', r'SH',
    # hónapok rövidítései
    r'\bJan\b', r'\bFeb\b', r'\bMar\b', r'\bApr\b', r'\bMay\b', r'\bJun\b',
    r'\bJul\b', r'\bAug\b', r'\bSep\b', r'\bOct\b', r'\bNov\b', r'\bDec\b',
    # napok számozása, pl. 1-6, 15-31
    r'\b\d{1,2}-\d{1,2}\b',
    # opcionális: évszakok, extra szövegek
    r'Winter', r'Summer',
]


def is_complex_opening_hours(oh_value):
    """ Heuristic check to see if opening_hours is too complex for automatic processing. """
    for pattern in COMPLEX_OH_PATTERNS:
        if re.search(pattern, oh_value, re.IGNORECASE):
            return True
    return False

def ascii_numcoder(text):
    output = ''
    for i in text:
        if i in range(0, 10, 1):
            output += i
        else:
            output += str(ord(i))
    return output


def save_csv_file(path: str, file: str, data, message: str):
    """Save Pandas dataframe to a CSV file

    Args:
        path (str): Path of newly created CVS file
        file (str): Filename of newly created CVS file
        data (pd.DataFrame): Pandas dataframe to write
        message (str): Addtion information to display
    """
    try:
        # Save file to CSV file
        logging.info('Saving %s to file: %s', message, file)
        res = data.to_csv(os.path.join(path, file))
        logging.info('The %s was successfully saved', file)
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def add_osm_node(osm_id: int, node_data: dict, prefix: str = 'poi') -> dict:
    """Generate OpenStreetMap node header information as string

    Args:
        osm_id (int): OpenStreetMap ID
        node_data (dict): [description]
        prefix (str): Prefix for field names in database

    Returns:
        str: [description]
    """
    if node_data.get('osm_timestamp') is None:
        osm_timestamp = datetime.datetime.now()
    else:
        osm_timestamp = node_data.get('osm_timestamp')
    osm_version = '99999' if node_data.get('osm_version') is None else node_data.get('osm_version')
    osm_data = {'action': 'modify', 'id': str(osm_id),
                'lat': '{}'.format(node_data.get('{}_lat'.format(prefix))),
                'lon': '{}'.format(node_data.get('{}_lon'.format(prefix))),
                'user': '{}'.format('osm_poi_matchmaker'), 'uid': '{}'.format('8635934'),
                'version': '{}'.format(osm_version),
                'timestamp': TIMESTAMP_FORMAT.format(osm_timestamp, dfmt=DATE_FORMAT, tfmt=TIME_FORMAT)}
    logging.info('Created OSM data: {}'.format(osm_data))
    return osm_data


def list_osm_node(osm_id: int, node_data: dict, prefix='poi') -> dict:
    """Generate OpenStreetMap node header information as string

    Args:
        osm_id (int): OpenStreetMap ID
        node_data (dict): [description]
        prefix (str): Prefix for field names in database

    Returns:
        str: [description]
    """
    logging.debug(node_data)
    osm_user = 'osm_poi_matchmaker' if node_data.get('osm_user') is None else node_data.get('osm_user')
    osm_user_id = '8635934' if node_data.get('osm_user_id') is None else node_data.get('osm_user_id')
    if node_data.get('osm_timestamp') is None:
        osm_timestamp = datetime.datetime.now()
    else:
        osm_timestamp = node_data.get('osm_timestamp')
    osm_version = '99999' if node_data.get('osm_version') is None else node_data.get('osm_version')
    osm_data = {'id': str(osm_id),
                'lat': '{}'.format(node_data.get('{}_lat'.format(prefix))),
                'lon': '{}'.format(node_data.get('{}_lon'.format(prefix))),
                'user': '{}'.format(osm_user), 'uid': '{}'.format(osm_user_id), 'version': '{}'.format(osm_version),
                'timestamp': TIMESTAMP_FORMAT.format(osm_timestamp, dfmt=DATE_FORMAT, tfmt=TIME_FORMAT)}
    logging.debug(osm_data)
    return osm_data


def add_osm_way(osm_id: int, node_data: dict) -> dict:
    """Generate OpenStreetMap way header information as dictionary

    Args:
        osm_id (int): [description]
        node_data (dict): [description]

    Returns:
        str: [description]
    """
    if node_data.get('osm_timestamp') is None:
        osm_timestamp = datetime.datetime.now()
    else:
        osm_timestamp = node_data.get('osm_timestamp')
    osm_version = '99999' if node_data.get('osm_version') is None else node_data.get('osm_version')
    osm_data = {'action': 'modify', 'id': str(osm_id),
                'user': '{}'.format('osm_poi_matchmaker'), 'uid': '{}'.format('8635934'),
                'version': '{}'.format(osm_version),
                'timestamp': TIMESTAMP_FORMAT.format(osm_timestamp, dfmt=DATE_FORMAT, tfmt=TIME_FORMAT)}
    return osm_data


def add_osm_link_comment(osm_id: int, osm_type) -> str:
    """Create OpenStreetMap osm.org link from OSM object

    Args:
        osm_id (int): [description]
        osm_type ([type]): [description]

    Returns:
        str: [description]
    """
    osm_comment = ' OSM link: https://osm.org/{}/{} '.format(osm_type.name, str(osm_id))
    return osm_comment


def add_osm_coordinate_comment(lat: str, long: str, zoom_level: int = 18) -> str:
    """Create OpenStreetMap osm.org link coordinates

    Args:
        lat: latitude
        long: longitude
        zoom_level: map zoom level

    Returns:
        str: [description]
    """
    osm_comment = ' OSM link: https://osm.org/#map={}/{}/{} '.format(zoom_level, lat, long)
    return osm_comment


def generate_osm_xml(df, session=None):
    """Crete OpenStreetMap (OSM XML) file from passed Panda Dataframe

    Args:
        df ([type]): [description]
        session ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """
    db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                              config.get_database_writer_password(),
                                              config.get_database_writer_host(),
                                              config.get_database_writer_port(),
                                              config.get_database_poi_database()))
    pgsql_pool = db.pool
    session_factory = sessionmaker(pgsql_pool)
    scoped_session_instance = scoped_session(session_factory)
    session = scoped_session_instance()
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    default_osm_id = -1
    current_osm_id = default_osm_id
    added_nodes = []
    try:
        for index, row in df.iterrows():
            try:
                logging.info('Start processing: {}. item'.format(index + 1))
                logging.debug(row.to_string())
                tags = {}
                xml_node_tags = None
                osm_live_tags = {}
                main_data = {}
                current_osm_id = default_osm_id if row.get('osm_id') is None else row.get('osm_id')
                osm_version = '99999' if row.get('osm_version') is None else row.get('osm_version')
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # OSM Object type is node and select this for new node to where osm_node type is not defined (None)
                if row.get('osm_node') is None or row.get('osm_node') == OSM_object_type.node:
                    try:
                        logging.debug('Object type is node or new node.')
                        josm_object = 'n{}'.format(current_osm_id)
                        main_data = etree.SubElement(osm_xml_data, 'node', add_osm_node(current_osm_id, row))
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                elif row.get('osm_node') is not None and row.get('osm_node') == OSM_object_type.way:
                    try:
                        logging.debug('Object type is way.')
                        main_data = etree.SubElement(osm_xml_data, 'way', add_osm_way(current_osm_id, row))
                        josm_object = 'w{}'.format(current_osm_id)
                        # Add way nodes without any modification)
                        node_data = []
                        if row.get('osm_nodes') is not None:
                            for n in json.loads(row.get('osm_nodes')):
                                data = etree.SubElement(main_data, 'nd', ref=str(n))
                        if session is not None:
                            if row.get('osm_nodes') is not None:
                                # Go through the list except the last value (which is same as the first)
                                for n in json.loads(row.get('osm_nodes')):
                                    # Add nodes only when it is not already added.
                                    if n not in added_nodes:
                                        added_nodes.append(n)
                                        way_node = db.query_from_cache(n, OSM_object_type.node)
                                        if way_node is not None:
                                            node_data = etree.SubElement(osm_xml_data, 'node',
                                                                         list_osm_node(n, way_node, 'osm'))
                                            if node_data.get('osm_live_tags') is not None and \
                                                    node_data.get('osm_live_tags') != '':
                                                node_osm_live_tags = node_data.get('osm_live_tags')
                                                for k, v in sorted(node_osm_live_tags).items():
                                                    xml_node_tags = etree.SubElement(node_data, 'tag', k=k, v='{}'.format(v))
                    except TypeError as e:
                        logging.warning('Missing nodes on this way: %s.', row.get('osm_id'))
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                elif row.get('osm_node') is not None and row.get('osm_node') == OSM_object_type.relation:
                    try:
                        logging.debug('Object type is relation.')
                        main_data = etree.SubElement(osm_xml_data, 'relation', add_osm_way(current_osm_id, row))
                        josm_object = 'r{}'.format(current_osm_id)
                        relations = relationer(row.get('osm_nodes'))
                        for i in relations:
                            data = etree.SubElement(main_data, 'member', type=i.get('type'), ref=i.get('ref'),
                                                    role=i.get('role'))
                    except TypeError as e:
                        logging.warning('Missing nodes on this relation: %s.', row['osm_id'])
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            # Add already existing node, way, relation OpenStreetMap reference as comment
            try:
                logging.debug('Add OSM reference as comment.')
                if current_osm_id > 0:
                    osm_xml_data.append(etree.Comment(add_osm_link_comment(current_osm_id, row.get('osm_node'))))
                # Add original POI coordinates as comment
                comment = etree.Comment(' Original coordinates: {} '.format(row.get('poi_geom')))
                osm_xml_data.append(comment)
                logging.debug('Add OSM - POI distance as comment.')
                dst = row.get('poi_distance')
                if 'poi_distance' in row and dst is not None and not (isinstance(dst, float) and np.isnan(dst)):
                    comment = etree.Comment(' OSM <-> POI distance: {} m'.format(row.get('poi_distance')))
                else:
                    logging.debug('New POI, have not got distance data.')
                    comment = etree.Comment(' OSM <-> POI distance: Non exist \n {}'.format(
                        add_osm_coordinate_comment(row.get('poi_geom').x, row.get('poi_geom').y)))
                osm_xml_data.append(comment)
                if 'poi_good' in row and 'poi_bad' in row:
                    logging.debug('Add good/bad quality comments.')
                    comment = etree.Comment(' Checker good: {}; bad {}'.format(row.get('poi_good'), row.get('poi_bad')))
                    osm_xml_data.append(comment)
                # Using already defined OSM tags if exists
                logging.debug('Add OSM live tags.')
                if row.get('osm_live_tags') is not None:
                    logging.debug('Add OSM live tags.')
                    tags_keys = list(osm_live_tags.keys())
                    for tk in tags_keys :
                        if tk.startswith('socket:'):
                            osm_live_tags.pop(tk)
                    logging.info('Additional live tags'.format(row.get('osm_live_tags')))
                    tags.update(row.get('osm_live_tags').copy())
                    osm_live_tags.update(row.get('osm_live_tags').copy())
                # Adding POI common tags
                logging.debug('Add POI common tags.')
                if row.get('poi_tags') is not None:
                    tags.update(row.get('poi_tags'))
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # Save live name tags if preserve name is enabled
                logging.debug('Preserve item name tag.')
                if row.get('preserve_original_name') is True and tags.get('name') is not None:
                    preserved_name = tags.get('name')
            except KeyError as e:
                logging.debug('No name tag is specified to save in original OpenStreetMap data.')
            try:
                # Overwriting with data from data providers
                logging.debug('Overwrite item tags from common tags.')
                for k, v in POI_TAGS.items():
                    if row.get(k) is not None:
                        if k != 'poi_addr_housenumber':
                            tags[v] = row.get(k)
                        else:
                            # Avoid housenumber change if only latter case is different (issue #129)
                            # 'poi_addr_housenumber': 'addr:housenumber'
                            if osm_live_tags.get(v) and row.get(k) and \
                                    osm_live_tags.get(v).lower() == row.get(k).lower():
                                tags[v] = osm_live_tags.get(v)
                            else:
                                tags[v] = row.get(k)
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                logging.debug('Decide opening_hours tag key.')
                if config.get_geo_alternative_opening_hours():
                    # Alternative opening hours code path to handle cases like COVID-19 special tagging
                    alternative_oh_tag = config.get_geo_alternative_opening_hours_tag()

                    if tags.get('opening_hours') is not None and tags.get('opening_hours') != '':
                        current_oh = tags.get('opening_hours')
                        if is_complex_opening_hours(current_oh):
                            # Too complex → not to modify, also add a comment
                            comment = f'Opening Hours is too complex ({current_oh}) to handle automatically.'
                            etree.Comment(comment)
                            logging.debug(f'Opening_hours too complex ({current_oh}), skipping modification.')
                        else:
                            if row.get('poi_opening_hours') is not None and row.get('poi_opening_hours') != '':
                                if current_oh == row.get('poi_opening_hours'):
                                    tags[alternative_oh_tag] = 'same'
                                else:
                                    tags[alternative_oh_tag] = row.get('poi_opening_hours')
                    else:
                        if row.get('poi_opening_hours') is not None and row.get('poi_opening_hours') != '':
                            tags['opening_hours'] = row.get('poi_opening_hours')
                            tags[alternative_oh_tag] = 'same'

                else:
                    # NON COVID-19 code path: simple opening hours tagging
                    if row.get('poi_opening_hours') is not None and row.get('poi_opening_hours') != '':
                        if tags.get('opening_hours') is not None and tags.get('opening_hours') != '':
                            current_oh = tags.get('opening_hours')
                            if is_complex_opening_hours(current_oh):
                                comment = f'Opening Hours is too complex ({current_oh}) to handle automatically.'
                                etree.Comment(comment)
                                logging.debug(f'Opening_hours too complex ({current_oh}), skipping modification.')
                            else:
                                tags['opening_hours'] = row.get('poi_opening_hours')
                        else:
                            tags['opening_hours'] = row.get('poi_opening_hours')

            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # If we got POI phone tag use it as OSM contact:phone tag
                logging.debug('Add contact:phone tag with phone numbers.')
                if row.get('poi_phone') is not None and row.get('poi_phone') != '':
                    tags['contact:phone'] = row.get('poi_phone')
                logging.debug('Add contact:mobil tag with phone numbers.')
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                logging.debug('Add contact:mobil tag with phone numbers.')
                if row.get('poi_mobil') is not None and row.get('poi_mobil') != '':
                    tags['contact:mobil'] = row.get('poi_mobil')
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                if row.get('do_not_export_addr_tags'):
                    logging.debug('Removing address tags based on common file settings')
                    tags_remove = ['addr:postcode', 'addr:city', 'addr:street', 'addr:housenumber',
                                   'addr:conscriptionnumber']
                    for tr in tags_remove:
                        tr_removed = []
                        if tr in tags:
                            tags.pop(tr, None)
                            tr_removed.append(tr)
                    if len(tr_removed) >= 1:
                        logging.debug('Removed tags are: {}'.format(','.join(tr_removed)))
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                if row.get('poi_ref'):
                    tags['ref'] = row.get('poi_ref')
                    logging.debug('Added ref tag.')
                    
                # If there is additional_ref_name then use it as key and poi_additional_ref as value
                if row.get('additional_ref_name') is not None and row.get('poi_additional_ref') is not None:
                    tags['ref:{}'.format(row.get('additional_ref_name'))] = row.get('poi_additional_ref')
                    logging.debug('Add ref:{} tag with additional ref name.'.format(row.get('additional_ref_name')))
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # If we got POI website tag use it as OSM contact:website tag
                logging.debug('Add contact:website tag with website URL.')
                tags['contact:website'] = url_tag_generator(row.get('poi_url_base'), row.get('poi_website'))
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # Short URL for source OSM tag
                # Can disable in app.conf via use.general.source.website.date key (deafault)
                logging.debug('Add source OSM tag.')
                if row['poi_url_base'] is not None and config.get_use_general_source_website_date() is False:
                    source_url = 'source:{}:date'.format(row.get('poi_url_base').split('/')[2])
                else:
                    # Issue #97: Remove source:(website):date tag and add source:website:date tag
                    # Then make it configurable via issue #98
                    if config.get_use_general_source_website_date_tag() is None:
                        source_url = 'source:date'
                    else:
                        source_url = config.get_use_general_source_website_date_tag()
                    if row['poi_url_base'] is not None:
                        source_url_2 = 'source:{}:date'.format(row.get('poi_url_base').split('/')[2])
                        tags.pop(source_url_2, None)
                tags[source_url] = '{:{dfmt}}'.format(datetime.datetime.now(), dfmt=DATE_FORMAT)
                # Add source tag, issue #101 and not just overwrite as #103
                if tags.get('source') is None:
                    tags['source'] = 'import;website'
                else:
                    for st in ['import', 'website']:
                        if st not in tags['source']:
                            tags['source'] = '{};{}'.format(tags['source'], st)
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # Write back the saved name tag
                if row.get('poi_type') != 'bus_stop':
                    if 'preserved_name' in locals():
                        logging.debug('Add back "{}" preserved name instead of common name.'.format(preserved_name))
                        tags['name'] = preserved_name
                    elif row.get('name') is not None:
                        logging.debug('Add "{}" individual name instead of common name.'.format(row.get('name')))
                        tags['name'] = row.get('name')
                else:
                    # Use OSM live 'name' tag for bus_stops when possible
                    if osm_live_tags.get('name') is not None and osm_live_tags.get('name') != '':
                        tags['name'] = osm_live_tags.get('name')
                    else:
                        if 'preserved_name' in locals():
                            tags['name'] = preserved_name
                        else:
                            tags['name'] = row.get('name')
                # Rewrite old contact tags to contact:* tag form
                logging.debug('Rewrite old contact tags to contact:* tag form.')
                tags_rewrite = ['website', 'phone', 'email', 'facebook', 'instagram', 'youtube', 'pinterest', 'fax', 'mobil']
                for tr in tags_rewrite:
                    if tr in tags:
                        # Never overwrite already existing contact:* tags
                        if 'contact:' + tr in tags:
                            # We already have this contact:* tag so remove the simple contact tag
                            tags.pop(tr, None)
                        else:
                            # Rewrite simple contact tag to contact:* tag
                            tags['contact:' + tr] = tags.pop(tr, None)
                        # rewrite email and website as small caps
                        if tr in ['email', 'website']:
                            if isinstance(tags.get('contact:{}'.format(tr)), str):
                                tags['contact:{}'.format(tr)] = str(tags.get('contact:{}'.format(tr))).lower()
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                logging.debug('Add description OSM tag.')
                if row.get('poi_description') is not None and row.get('poi_description') != '':
                    tags['description'] = row.get('poi_description')
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # Write tags with yes/no value
                logging.debug('Add boolean OSM tags.')
                for k, v in POI_YESNO_TAGS.items():
                    if row.get(k) is not None and row.get(k) != '':
                        tags[v] = 'yes' if row.get(k) is True else 'no'

                logging.debug('Add EV tags')
                for k, v in POI_EV_TAGS.items():
                    if isinstance(row.get(k), float):
                        if not math.isnan(row.get(k)):
                            tags[v] = int(row.get(k))
                    else:
                        if row.get(k) is not None:
                            tags[v] = row.get(k)
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # This is a new POI - will add fix me tag to the new items.
                if row.get('poi_new') is not None and row.get('poi_new') is True:
                    logging.debug('Add "fixme:verify import" OSM tag for new item.')
                    tags['fixme'] = 'Ellenőrizni a meglétét és a pontos helyére tenni'
                # Remove unwanted addr:country from file output as we discussed in Issue #33
                logging.debug('Remove addr:country OSM tag.')
                tags.pop('addr:country', None)
                # tags['import'] = 'osm_poi_matchmaker'
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                if row.get('poi_type') == 'bus_stop':
                    tags.pop('ref:vatin', None)
                    tags.pop('ref:vatin:hu', None)
                    tags.pop('ref:vatin:HU', None)
                    tags.pop('ref:HU:vatin', None)
                    tags.pop('ref:hu:vatin', None)
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                # Remove name tag in export if export_poi_name is false
                if row.get('export_poi_name') is False:
                    tags.pop('name', None)
                if tags.get('name') == 'None':
                    tags.pop('name', None)
            except KeyError as e:
                logging.debug('No name tag is specified - do not have to remove.')
            try:
                # Rendering tags to the XML file and JOSM magic link
                logging.debug('Rendering OSM tag as XML comments.')
                josm_link = ''
                comment = '\nKey\t\t\t\tStatus\t\tNew value\t\tOSM value\n'
                for k, v in sorted(tags.items()):
                    xml_tags = etree.SubElement(main_data, 'tag', k=k, v='{}'.format(v))
                    josm_link = '{}|{}={}'.format(josm_link, k, v)
                    # Add original POI tags as comment
                    try:
                        if isinstance(v, str):
                            v = v.replace('-', '\-').replace('\n', '')
                        w = osm_live_tags[k]
                    except KeyError:
                        comment += "{:32} NEW\t\t'{}'\n".format(k, v)
                    else:
                        if isinstance(w, str):
                            w = w.replace('-', '\-').replace('\n', '')
                        comment += "{:32} {}\t\t'{}'\t\t\t'{}'\n".format(k, compare_strings(v, w), v, w)
                logging.debug('Adding OSM tag XML comments to XML file representation.')
                comment = etree.Comment(comment)
                osm_xml_data.append(comment)
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                logging.debug('Rendering JOSM link as XML comment.')
                # URL encode link and '--' in comment
                josm_link = quote(josm_link)
                josm_link = josm_link.replace('--', '%2D%2D')
                logging.debug('Adding JOSM link as XML comments to XML file representation.')
                comment = etree.Comment(' JOSM magic link: {}?new_layer=false&objects={}&addtags={} '.format
                                        ('http://localhost:8111/load_object', josm_object, josm_link))
                osm_xml_data.append(comment)
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            try:
                logging.debug('Rendering test data as XML comment.')
                test_case = {ckey: (row.get(ckey, None).replace('-', '\-') if isinstance(row.get(ckey, None), str) \
                    else row.get(ckey, None)) for ckey in TESTCASE_GEN_KEYS}
                comment = etree.Comment(
                    "ˇ'original': '{t[original]}', 'postcode': '{t[poi_postcode]}', 'city': '{t[poi_city]}', 'street': '{t[poi_addr_street]}', 'housenumber': '{t[poi_addr_housenumber]}', 'conscriptionnumber': '{t[poi_conscriptionnumber]}'°".format(
                        t=test_case))
                osm_xml_data.append(comment)
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
                logging.error(test_case)
            try:
                logging.debug('XML node tags check.')
                if xml_node_tags is not None:
                    logging.debug('Add XML node tags.')
                    osm_xml_data.append(xml_node_tags)
            except UnboundLocalError as e:
                logging.debug('Unbound local error extra node tags')
                logging.exception(traceback.format_exc())
                logging.debug(etree.dump(osm_xml_data))
                logging.debug(etree.dump(xml_node_tags))
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
                logging.debug(etree.dump(osm_xml_data))
                logging.debug(etree.dump(xml_node_tags))
            try:
                logging.debug('Attach POI data to main XML node.')
                osm_xml_data.append(main_data)
                # Next default OSM id is one more less for non existing objects
                logging.debug('Decrease OSM id (negative number) of new POI for the next round.')
                default_osm_id -= 1
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
            # Not use preserved name for next item
            if 'preserved_name' in locals():
                del preserved_name
            logging.info('Finished processing: {}. item'.format(index + 1))
        logging.debug('All items have processed.')
    except ValueError as e:
        logging.exception('ValueError Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())

    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())
    logging.info('Finished process of {} items'.format(len(df)))
    #logging.debug('---------------------------------------------')
    #logging.debug(lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8"))
    #logging.debug('---------------------------------------------')
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")
