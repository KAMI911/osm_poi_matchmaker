# -*- coding: utf-8 -*-

try:
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


def generate_osm_xml(pd):
    from lxml import etree
    import lxml
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    id = -1
    for index, row in pd.iterrows():
        '''
        data = etree.SubElement(osm_xml_data, 'node', action='new', id='{}'.format(row['osm_id']),
                                lat='{}'.format(row['poi_lat']), lon='{}'.format(row['poi_lon']),
                                user='{}'.format(row['osm_user']), timestamp='{}'.format(row['osm_timestamp']),
                                uid='{}'.format(row['osm_uid']), changeset='{}'.format(row['osm_changeset']),
                                version='{}'.format(row['osm_version']))
        '''
        data = etree.SubElement(osm_xml_data, 'node', action='modify', id=str(id),
                                lat='{}'.format(row['poi_lat']), lon='{}'.format(row['poi_lon']))
        # comment = etree.Comment(' Stop name: {0}, ID: {1} '.format(row['stop_name'], row['osm_merged_refs']))
        # data.append(comment)
        if row['poi_name'] is not None:
            tags = etree.SubElement(data, 'tag', k='name', v='{}'.format(row['poi_name']))
        if row['poi_city'] is not None:
            tags = etree.SubElement(data, 'tag', k='addr:city', v='{}'.format(row['poi_city']))
        if row['poi_postcode'] is not None:
            tags = etree.SubElement(data, 'tag', k='addr:postcode', v='{}'.format(row['poi_postcode']))
        if row['poi_addr_street'] is not None:
            tags = etree.SubElement(data, 'tag', k='addr:street', v='{}'.format(row['poi_addr_street']))
        if row['poi_addr_housenumber'] is not None:
            tags = etree.SubElement(data, 'tag', k='addr:housenumber', v='{}'.format(row['poi_addr_housenumber']))
        if row['poi_conscriptionnumber'] is not None:
            tags = etree.SubElement(data, 'tag', k='addr:conscriptionnumber', v='{}'.format(row['poi_conscriptionnumber']))
        if row['poi_branch'] is not None:
            tags = etree.SubElement(data, 'tag', k='branch', v='{}'.format(row['poi_branch']))
        if row['poi_url_base'] is not None and row['poi_website'] is not None:
            tags = etree.SubElement(data, 'tag', k='contact:website', v='{}{}'.format(row['poi_url_base'], row['poi_website']))
        elif row['poi_url_base'] is not None:
            tags = etree.SubElement(data, 'tag', k='contact:website', v='{}'.format(row['poi_url_base']))
        osm_xml_data.append(data)
        id -= 1
        try:
            for k, v in eval(row['poi_tags']).items():
                tags = etree.SubElement(data, 'tag', k='{}'.format(k), v='{}'.format(v))
                osm_xml_data.append(data)
        except NameError as err:
            logging.warning('Unable to eval tags: {}'.format(row['poi_tags']))
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")

