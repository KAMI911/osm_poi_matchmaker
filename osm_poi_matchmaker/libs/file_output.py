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


def generate_uic_xml(pd, looking_for):
    from lxml import etree
    import lxml
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    for index, row in pd.iterrows():
        data = etree.SubElement(osm_xml_data, 'node', action='modify', id='{}'.format(row['osm_id']),
                                lat='{}'.format(row['osm_lat']), lon='{}'.format(row['osm_lon']),
                                user='{}'.format(row['osm_user']), timestamp='{}'.format(row['osm_timestamp']),
                                uid='{}'.format(row['osm_uid']), changeset='{}'.format(row['osm_changeset']),
                                version='{}'.format(row['osm_version']))
        # comment = etree.Comment(' Stop name: {0}, ID: {1} '.format(row['stop_name'], row['uic_ref']))
        # data.append(comment)
        if 'railway' in looking_for:
            row['osm_tags']['uic_ref'] = '{}{:05d}'.format(row['Országkód'], row['Állomáskód'])
            row['osm_tags']['uic_name'] = row['Név']
        for k, v in row['osm_tags'].items():
            tags = etree.SubElement(data, 'tag', k='{}'.format(k), v='{}'.format(v))
            osm_xml_data.append(data)
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")


def generate_stop_xml(pd, looking_for):
    from lxml import etree
    import lxml
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    for index, row in pd.iterrows():
        data = etree.SubElement(osm_xml_data, 'node', action='modify', id='{}'.format(row['osm_id']),
                                lat='{}'.format(row['osm_lat']), lon='{}'.format(row['osm_lon']),
                                user='{}'.format(row['osm_user']), timestamp='{}'.format(row['osm_timestamp']),
                                uid='{}'.format(row['osm_uid']), changeset='{}'.format(row['osm_changeset']),
                                version='{}'.format(row['osm_version']))
        comment = etree.Comment(' Stop name: {0}, ID: {1} '.format(row['stop_name'], row['osm_merged_refs']))
        data.append(comment)
        if 'railway' in looking_for:
            row['osm_tags']['ref:mav'] = row['stop_id']
        for k, v in row['osm_tags'].items():
            tags = etree.SubElement(data, 'tag', k='{}'.format(k), v='{}'.format(v))
            osm_xml_data.append(data)
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")


def generate_shape_xml(pd):
    from lxml import etree
    import lxml
    osm_xml_data = etree.Element('osm', version='0.6', generator='JOSM')
    old_shape_id = 0
    for index, row in pd.iterrows():
        data = etree.SubElement(osm_xml_data, 'node', action='modify',
                                id='-{}{}'.format(ascii_numcoder(row['shape_id']), row['shape_pt_sequence']),
                                lat='{}'.format(row['shape_pt_lat']), lon='{}'.format(row['shape_pt_lon']))
    for index, row in pd.iterrows():
        if old_shape_id != ascii_numcoder(row['shape_id']):
            print('{}   {} not same'.format(old_shape_id, ascii_numcoder(row['shape_id'])))
            way = etree.SubElement(osm_xml_data, 'way', id='-{}'.format(ascii_numcoder(row['shape_id'])))
            old_shape_id = ascii_numcoder(row['shape_id'])
        nd = etree.SubElement(way, 'nd', ref='-{}{}'.format(ascii_numcoder(row['shape_id']), row['shape_pt_sequence']))
    osm_xml_data.append(way)
    return lxml.etree.tostring(osm_xml_data, pretty_print=True, xml_declaration=True, encoding="UTF-8")
