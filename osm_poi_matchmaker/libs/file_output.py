# -*- coding: utf-8 -*-

try:
    import math
    import logging
    import os
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


def ascii_numcoder(text):
    output = ''
    for i in text:
        if i in range(0, 10, 1):
            output += i
        else:
            output += str(ord(i))
    return output


def save_csv_file(path, file, data, message):
    # Save file to CSV file
    logging.info('Saving {0} to file: {1}'.format(message, file))
    res = data.to_csv(os.path.join(path, file))
    logging.info('The {0} was sucessfully saved'.format(file))


def generate_osm_xml(df):
    from lxml import etree
    import lxml
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    id = -1
    current_id = id
    for index, row in df.iterrows():
        current_id = id if row['osm_id'] is None else row['osm_id']
        osm_timestamp = '' if row['osm_timestamp'] is None else row['osm_timestamp']
        osm_changeset = '99999' if row['osm_changeset'] is None else row['osm_changeset'] + 1
        osm_version = '99999' if row['osm_version'] is None else row['osm_version']
        if row['node'] is None or (row['node'] == True or math.isnan(row['node'])):
            main_data = etree.SubElement(osm_xml_data, 'node', action='modify', id=str(current_id),
                                lat='{}'.format(row['poi_lat']), lon='{}'.format(row['poi_lon']),
                                user='{}'.format('KAMI'), timestamp='{}'.format(osm_timestamp),
                                uid='{}'.format('4579407'), changeset='{}'.format(osm_changeset),
                                version='{}'.format(osm_version))
        elif row['node'] is not None and row['node'] == False:
            main_data = etree.SubElement(osm_xml_data, 'way', action='modify', id=str(current_id),
                                    user='{}'.format('KAMI'), timestamp='{}'.format(osm_timestamp),
                                    uid='{}'.format('4579407'), changeset='{}'.format(osm_changeset),
                                    version='{}'.format(osm_version))
            # Add way nodes without any modification)
            try:
                for n in row['osm_nodes']:
                    data = etree.SubElement(main_data, 'nd', ref=str(n))
            except TypeError as err:
                logging.warning('Missing nodes on this way: {}.'.format(row['osm_id']))
        # Using already definied OSM tags if exists
        if row['osm_live_tags'] is not None:
            tags = row['osm_live_tags']
        else:
            tags = {}
        # Overwriting with data from data providers
        if row['poi_name'] is not None:
            tags['name'] = row['poi_name']
        if row['poi_city'] is not None:
            tags['addr:city'] = row['poi_city']
        if row['poi_postcode'] is not None:
            tags['addr:postcode'] = row['poi_postcode']
        if row['poi_addr_street'] is not None:
            tags['addr:street'] = row['poi_addr_street']
        if row['poi_addr_housenumber'] is not None:
            tags['addr:housenumber'] = row['poi_addr_housenumber']
        if row['poi_conscriptionnumber'] is not None:
            tags['addr:conscriptionnumber'] = row['poi_conscriptionnumber']
        if row['poi_branch'] is not None:
            tags['branch'] = row['poi_branch']
        if row['poi_email'] is not None:
            tags['email'] = row['poi_email']
        if row['poi_phone'] is not None and not math.isnan(row['poi_phone']):
            tags['phone'] = '+{:d}'.format(int(row['poi_phone']))
        if row['poi_url_base'] is not None and row['poi_website'] is not None:
            tags['website'] = '{}{}'.format(row['poi_url_base'], row['poi_website'])
        elif row['poi_url_base'] is not None:
            tags['website'] = row['poi_url_base']
        # Adding POI common tags
        if row['poi_tags'] is not None:
            print(eval(row['poi_tags']))
            tags.update(eval(row['poi_tags']))
        # Rendering tags to XML file
        for k, v in tags.items():
            xml_tags = etree.SubElement(main_data, 'tag', k=k, v='{}'.format(v))
        osm_xml_data.append(main_data)
        id -= 1
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")

