# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import traceback
    from osm_poi_matchmaker.libs.file_output import save_csv_file, generate_osm_xml, generate_geojson
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def export_raw_poi_data(addr_data, comm_data, postfix=''):
    try:
        logging.info('Exporting CSV files ...')
        # And merge and them into one Dataframe and save it to a CSV file
        save_csv_file(config.get_directory_output(), 'poi_common{}.csv'.format(postfix), comm_data, 'poi_common')
        save_csv_file(config.get_directory_output(), 'poi_address{}.csv'.format(postfix), addr_data, 'poi_address')
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_raw_poi_data_xml(addr_data, postfix=''):
    try:
        with open(os.path.join(config.get_directory_output(), 'poi_address{}.osm'.format(postfix)), 'wb') as oxf:
            oxf.write(generate_osm_xml(addr_data))
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data(data):
    try:
        # Generating CSV, OSM XML and GeoJSON files grouped by poi_code
        output_dir = data[0]
        filename = data[1]
        rows = data[2]
        table = data[3]
        save_csv_file(output_dir, '{}.csv'.format(filename), rows, table)
        with open(os.path.join(output_dir, '{}.osm'.format(filename)), 'wb') as oxf:
            try:
                logging.info('Saving {} to file: {}.osm'.format(table, filename))
                oxf.write(generate_osm_xml(rows))
                logging.info('The {}.osm was successfully saved'.format(filename))
            except Exception as e:
                logging.exception('Exception occurred during write: {}'.format(e))
                logging.exception(traceback.format_exc())
        with open(os.path.join(output_dir, '{}.geojson'.format(filename)), 'wb') as gf:
            try:
                logging.info('Saving {} to file: {}.geojson'.format(table, filename))
                gf.write(generate_geojson(rows))
                logging.info('The {}.geojson was successfully saved'.format(filename))
            except Exception as e:
                logging.exception('Exception occurred during GeoJSON write: {}'.format(e))
                logging.exception(traceback.format_exc())
    except Exception as e:
        logging.exception('Exception occurred during opening file: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_raw_poi_data_geojson(addr_data, postfix=''):
    try:
        geojson_path = os.path.join(config.get_directory_output(), 'poi_address{}.geojson'.format(postfix))
        logging.info('Saving GeoJSON to file: poi_address%s.geojson', postfix)
        with open(geojson_path, 'wb') as gf:
            gf.write(generate_geojson(addr_data))
        logging.info('The poi_address%s.geojson was successfully saved', postfix)
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data_geojson(data):
    try:
        output_dir = data[0]
        filename = data[1]
        rows = data[2]
        geojson_path = os.path.join(output_dir, '{}.geojson'.format(filename))
        logging.info('Saving GeoJSON to file: %s.geojson', filename)
        with open(geojson_path, 'wb') as gf:
            gf.write(generate_geojson(rows))
        logging.info('The %s.geojson was successfully saved', filename)
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_new_poi_data(addr_data, postfix=''):
    """Export only new (unmatched) POIs — those with no osm_id — to XML and GeoJSON."""
    try:
        new_data = addr_data[addr_data['osm_id'].isna()]
        if len(new_data) == 0:
            logging.info('No new POIs to export.')
            return
        logging.info('Exporting %d new POIs.', len(new_data))
        with open(os.path.join(config.get_directory_output(),
                               'poi_address_new{}.osm'.format(postfix)), 'wb') as oxf:
            oxf.write(generate_osm_xml(new_data))
        with open(os.path.join(config.get_directory_output(),
                               'poi_address_new{}.geojson'.format(postfix)), 'wb') as gf:
            gf.write(generate_geojson(new_data))
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_existing_poi_data(addr_data, postfix=''):
    """Export only existing (matched) POIs — those with an osm_id — to XML and GeoJSON."""
    try:
        existing_data = addr_data[addr_data['osm_id'].notna()]
        if len(existing_data) == 0:
            logging.info('No existing POIs to export.')
            return
        logging.info('Exporting %d existing POIs.', len(existing_data))
        with open(os.path.join(config.get_directory_output(),
                               'poi_address_existing{}.osm'.format(postfix)), 'wb') as oxf:
            oxf.write(generate_osm_xml(existing_data))
        with open(os.path.join(config.get_directory_output(),
                               'poi_address_existing{}.geojson'.format(postfix)), 'wb') as gf:
            gf.write(generate_geojson(existing_data))
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data_new(data):
    """Per-poi_code export of new (unmatched) POIs to XML and GeoJSON."""
    try:
        output_dir = data[0]
        filename = data[1]
        rows = data[2]
        new_rows = rows[rows['osm_id'].isna()]
        if len(new_rows) == 0:
            return
        with open(os.path.join(output_dir, '{}.osm'.format(filename)), 'wb') as oxf:
            oxf.write(generate_osm_xml(new_rows))
        with open(os.path.join(output_dir, '{}.geojson'.format(filename)), 'wb') as gf:
            gf.write(generate_geojson(new_rows))
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data_existing(data):
    """Per-poi_code export of existing (matched) POIs to XML and GeoJSON."""
    try:
        output_dir = data[0]
        filename = data[1]
        rows = data[2]
        existing_rows = rows[rows['osm_id'].notna()]
        if len(existing_rows) == 0:
            return
        with open(os.path.join(output_dir, '{}.osm'.format(filename)), 'wb') as oxf:
            oxf.write(generate_osm_xml(existing_rows))
        with open(os.path.join(output_dir, '{}.geojson'.format(filename)), 'wb') as gf:
            gf.write(generate_geojson(existing_rows))
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data_with_postcode_groups(data):
    try:
        # Generating CSV files group by poi_code and postcode
        output_dir = data[0]
        filename = data[1]
        rows = data[2].sort_values(by=['poi_postcode'], na_position='last').reset_index(drop=True)
        # Maximum number of items in one file
        batch = 100
        # Minimum difference between postcode grouped data sets
        postcode_gap = 200
        # Postcode maximum value
        postcode_stop = len(rows)
        if postcode_stop > batch:
            # Create sliced data output
            for i in range(0, postcode_stop, postcode_gap):
                stop = i + postcode_gap - 1
                xml_export = rows[i:stop]
                if len(xml_export) != 0:
                    with open(os.path.join(output_dir, '{}_{:04d}-{:04d}.osm'.format(filename, i, stop)), 'wb') as oxf:
                        oxf.write(generate_osm_xml(xml_export))
    except Exception as e:
        logging.error(e)
        logging.exception('Exception occurred')
