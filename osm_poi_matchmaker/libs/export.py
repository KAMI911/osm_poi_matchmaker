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


def _write_osm_and_geojson(output_dir, filename, rows):
    """Write OSM XML and GeoJSON files to output_dir/{filename}.osm and .geojson."""
    with open(os.path.join(output_dir, '{}.osm'.format(filename)), 'wb') as oxf:
        oxf.write(generate_osm_xml(rows))
    with open(os.path.join(output_dir, '{}.geojson'.format(filename)), 'wb') as gf:
        gf.write(generate_geojson(rows))


def export_raw_poi_data(addr_data, comm_data, postfix=''):
    """Save the full (ungrouped) POI and common-type dataframes as CSV files
    (poi_address{postfix}.csv, poi_common{postfix}.csv) - not filtered by
    matched/new status, unlike export_new_poi_data()/export_existing_poi_data()."""
    try:
        logging.info('Exporting CSV files ...')
        # And merge and them into one Dataframe and save it to a CSV file
        save_csv_file(config.get_directory_output(), 'poi_common{}.csv'.format(postfix), comm_data, 'poi_common')
        save_csv_file(config.get_directory_output(), 'poi_address{}.csv'.format(postfix), addr_data, 'poi_address')
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_raw_poi_data_xml(addr_data, postfix=''):
    """Save the full (ungrouped) POI dataframe as one OSM XML file
    (poi_address{postfix}.osm)."""
    try:
        with open(os.path.join(config.get_directory_output(), 'poi_address{}.osm'.format(postfix)), 'wb') as oxf:
            oxf.write(generate_osm_xml(addr_data))
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_raw_poi_data_geojson(addr_data, postfix=''):
    """Save the full (ungrouped) POI dataframe as one GeoJSON file
    (poi_address{postfix}.geojson)."""
    try:
        geojson_path = os.path.join(config.get_directory_output(), 'poi_address{}.geojson'.format(postfix))
        logging.info('Saving GeoJSON to file: poi_address%s.geojson', postfix)
        with open(geojson_path, 'wb') as gf:
            gf.write(generate_geojson(addr_data))
        logging.info('The poi_address%s.geojson was successfully saved', postfix)
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data(data):
    """Save one poi_code's rows as CSV, OSM XML and GeoJSON files. Used as the
    per-group worker function for the multiprocessing pool in
    create_db.py's WorkflowManager.start_exporter().

    Args:
        data (list): [output_dir, filename (without extension), rows (DataFrame),
            table name to log (e.g. 'poi_address')].
    """
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


def _export_split_by_match_status(rows, matched, output_dir, filename, empty_log=None, count_log=None):
    """Filter rows by whether they have an osm_id (matched) or not (new), and write
    the result via _write_osm_and_geojson() if non-empty. Shared by
    export_new_poi_data()/export_existing_poi_data() and their per-poi_code grouped
    counterparts, which differ only in the osm_id direction and log messages.

    Args:
        rows (pandas.DataFrame): POI rows to filter, must have an osm_id column.
        matched (bool): True to keep rows with an osm_id set, False to keep rows
            with no osm_id.
        output_dir (str): Directory to write the .osm/.geojson files into.
        filename (str): Output filename (without extension).
        empty_log (str, optional): Message logged (info level) if nothing matches.
        count_log (str, optional): '%d'-style message logged with the row count if
            anything matches.
    """
    filtered = rows[rows['osm_id'].notna()] if matched else rows[rows['osm_id'].isna()]
    if len(filtered) == 0:
        if empty_log:
            logging.info(empty_log)
        return
    if count_log:
        logging.info(count_log, len(filtered))
    _write_osm_and_geojson(output_dir, filename, filtered)


def export_new_poi_data(addr_data, postfix=''):
    """Export only new (unmatched) POIs — those with no osm_id — to XML and GeoJSON."""
    if addr_data is None:
        logging.warning('No addr_data provided to export_new_poi_data.')
        return
    try:
        _export_split_by_match_status(addr_data, False, config.get_directory_output(),
                                      'poi_address_new{}'.format(postfix),
                                      empty_log='No new POIs to export.', count_log='Exporting %d new POIs.')
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_existing_poi_data(addr_data, postfix=''):
    """Export only existing (matched) POIs — those with an osm_id — to XML and GeoJSON."""
    if addr_data is None:
        logging.warning('No addr_data provided to export_existing_poi_data.')
        return
    try:
        _export_split_by_match_status(addr_data, True, config.get_directory_output(),
                                      'poi_address_existing{}'.format(postfix),
                                      empty_log='No existing POIs to export.',
                                      count_log='Exporting %d existing POIs.')
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data_new(data):
    """Per-poi_code export of new (unmatched) POIs to XML and GeoJSON."""
    try:
        output_dir, filename, rows = data[0], data[1], data[2]
        _export_split_by_match_status(rows, False, output_dir, filename)
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data_existing(data):
    """Per-poi_code export of existing (matched) POIs to XML and GeoJSON."""
    try:
        output_dir, filename, rows = data[0], data[1], data[2]
        _export_split_by_match_status(rows, True, output_dir, filename)
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


def export_grouped_poi_data_with_postcode_groups(data):
    """Split one poi_code's rows (pre-sorted by postcode) into postcode-ordered
    chunks and save each as its own OSM XML file, so no single file gets too large
    for e.g. manual JOSM review. Only runs if there are more than 100 rows total.

    Args:
        data (list): [output_dir, filename prefix (without extension), rows
            (DataFrame with a poi_postcode column)].
    """
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
