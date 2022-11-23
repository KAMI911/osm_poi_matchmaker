# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import traceback
    from osm_poi_matchmaker.libs.file_output import save_csv_file, generate_osm_xml
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
        logging.exception(traceback.print_exc())


def export_raw_poi_data_xml(addr_data, postfix=''):
    try:
        with open(os.path.join(config.get_directory_output(), 'poi_address{}.osm'.format(postfix)), 'wb') as oxf:
            oxf.write(generate_osm_xml(addr_data))
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.print_exc())


def export_grouped_poi_data(data):
    try:
        # Generating CSV files group by poi_code
        output_dir = data[0]
        filename = data[1]
        rows = data[2]
        table = data[3]
        # Generating CSV files group by poi_code
        save_csv_file(output_dir, '{}.csv'.format(filename), rows, table)
        with open(os.path.join(output_dir, '{}.osm'.format(filename)), 'wb') as oxf:
            try:
                logging.info('Saving {} to file: {}.osm'.format(table, filename))
                oxf.write(generate_osm_xml(rows))
                logging.info('The {}.osm was sucessfully saved'.format(filename))
            except Exception as e:
                logging.exception('Exception occurred during write wile: {}'.format(e))
                logging.error(traceback.print_exc())
    except Exception as e:
        logging.exception('Exception occurred during opening file: {}'.format(e))
        logging.error(traceback.print_exc())


def export_grouped_poi_data_with_postcode_groups(data):
    try:
        # Generating CSV files group by poi_code and postcode
        output_dir = data[0]
        filename = data[1]
        rows = data[2]
        # Maximum number of items in one file
        batch = 100
        # Minimum difference between postcode grouped data sets
        postcode_gap = 200
        # Postcode minimum value
        postcode_start = 1000
        # Postcode maximum value
        postcode_stop = 9999
        if len(rows) > batch:
            # Create sliced data output
            for i in range(postcode_start, postcode_stop, postcode_gap):
                stop = i + postcode_gap - 1
                xml_export = rows[rows['poi_postcode'].between(int(i), int(stop), inclusive="both")]
                print(xml_export.to_string())
                if len(xml_export) != 0:
                    with open(os.path.join(output_dir, '{}_{:04d}-{:04d}.osm'.format(filename, i, stop)), 'wb') as oxf:
                        oxf.write(generate_osm_xml(xml_export))
                i += postcode_gap
    except Exception as e:
        logging.error(e)
        logging.exception('Exception occurred')
