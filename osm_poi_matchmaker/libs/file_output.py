# -*- coding: utf-8 -*-

try:
    import traceback
    import math
    import logging
    import os
    import datetime
    from urllib.parse import quote
    from osm_poi_matchmaker.dao.data_structure import OSM_object_type
    from osm_poi_matchmaker.libs.osm import relationer, timestamp_now
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
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
        print(err)


def generate_osm_xml(df):
    from lxml import etree
    import lxml
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    id = -1
    current_id = id
    try:
        for index, row in df.iterrows():
            current_id = id if row['osm_id'] is None else row['osm_id']
            osm_timestamp = timestamp_now() if row['osm_timestamp'] is None else row['osm_timestamp']
            osm_changeset = '99999' if row['osm_changeset'] is None else row['osm_changeset'] + 1
            osm_version = '99999' if row['osm_version'] is None else row['osm_version']
            if row['osm_node'] is None or row['osm_node'] == OSM_object_type.node:
                main_data = etree.SubElement(osm_xml_data, 'node', action='modify', id=str(current_id),
                                             lat='{}'.format(row['poi_lat']), lon='{}'.format(row['poi_lon']),
                                             user='{}'.format('KAMI'), timestamp='{}'.format(osm_timestamp),
                                             uid='{}'.format('4579407'), changeset='{}'.format(osm_changeset),
                                             version='{}'.format(osm_version))
                josm_object = 'n{}'.format(current_id)
                if current_id > 0:
                    comment = etree.Comment(' OSM link: https://osm.org/node/{} '.format(str(current_id)))
                    osm_xml_data.append(comment)
            elif row['osm_node'] is not None and row['osm_node'] == OSM_object_type.way:
                main_data = etree.SubElement(osm_xml_data, 'way', action='modify', id=str(current_id),
                                             user='{}'.format('KAMI'), timestamp='{}'.format(osm_timestamp),
                                             uid='{}'.format('4579407'), changeset='{}'.format(osm_changeset),
                                             version='{}'.format(osm_version))
                josm_object = 'w{}'.format(current_id)
                # Add way nodes without any modification)
                try:
                    for n in row['osm_nodes']:
                        data = etree.SubElement(main_data, 'nd', ref=str(n))
                except TypeError as err:
                    logging.warning('Missing nodes on this way: {}.'.format(row['osm_id']))
                # Add node reference as comment for existing POI
                if current_id > 0:
                    comment = etree.Comment(' OSM link: https://osm.org/way/{} '.format(str(current_id)))
                    osm_xml_data.append(comment)
            elif row['osm_node'] is not None and row['osm_node'] == OSM_object_type.relation:
                main_data = etree.SubElement(osm_xml_data, 'relation', action='modify', id=str(current_id),
                                             user='{}'.format('KAMI'), timestamp='{}'.format(osm_timestamp),
                                             uid='{}'.format('4579407'), changeset='{}'.format(osm_changeset),
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
            # Add original POI tags as comment
            comment = ''
            if row['osm_live_tags'] is not None:
                for k, v in sorted(row['osm_live_tags'].items()):
                    if isinstance(v, str):
                        v = v.replace('--', '\-\-').replace('\n', '')
                    comment += "'{}':'{}'; ".format(k, v)
            comment = etree.Comment(' Original tags: {} '.format(comment))
            osm_xml_data.append(comment)
            # Using already definied OSM tags if exists
            if row['osm_live_tags'] is not None:
                tags = row['osm_live_tags']
            else:
                tags = {}
            # Adding POI common tags
            if row['poi_tags'] is not None:
                tags.update(eval(row['poi_tags']))
            # Overwriting with data from data providers
            for k, v in POI_TAGS.items():
                if row[k] is not None:
                    tags[v] = row[k]
            if row['poi_phone'] is not None and not math.isnan(row['poi_phone']):
                tags['phone'] = '+{:d}'.format(int(row['poi_phone']))
            if row['poi_url_base'] is not None and row['poi_website'] is not None:
                tags['website'] = '{}{}'.format(row['poi_url_base'], row['poi_website'])
            elif row['poi_url_base'] is not None:
                tags['website'] = row['poi_url_base']
            # Short URL for source
            if row['poi_url_base'] is not None:
                source_url = 'source:{}:date'.format(row['poi_url_base'].split('/')[2])
            else:
                source_url = 'source:website:date'
            tags[source_url] = '{:{dfmt}}'.format(datetime.datetime.now(), dfmt='%Y-%m-%d')
            tags['import'] = 'osm_poi_matchmaker'
            # Rendering tags to the XML file and JOSM magic link
            josm_link = ''
            for k, v in sorted(tags.items()):
                xml_tags = etree.SubElement(main_data, 'tag', k=k, v='{}'.format(v))
                josm_link = '{}|{}={}'.format(josm_link, k, v)
            # URL encode link and '--' in comment
            josm_link = quote(josm_link)
            josm_link = josm_link.replace('--', '%2D%2D')
            comment = etree.Comment(' JOSM magic link: {}?new_layer=false&objects={}&addtags={} '.format('http://localhost:8111/load_object', josm_object, josm_link))
            osm_xml_data.append(comment)
            osm_xml_data.append(main_data)
            id -= 1
    except Exception as err:
        print(err)
        traceback.print_exc()
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")
