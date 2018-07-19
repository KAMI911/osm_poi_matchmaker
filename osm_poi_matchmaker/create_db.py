#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'kami911'

try:
    import os
    import traceback
    import logging
    import logging.config
    import sqlalchemy
    import sqlalchemy.orm
    import numpy as np
    import pandas as pd
    import geopandas as gpd
    import multiprocessing
    from osmapi import OsmApi
    from osm_poi_matchmaker.utils import config, timing, dataproviders_loader
    from osm_poi_matchmaker.libs.file_output import save_csv_file, generate_osm_xml
    from osm_poi_matchmaker.dao.data_handlers import insert_type
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osm_poi_matchmaker.dao.poi_base import POIBase
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

__program__ = 'create_db'
__version__ = '0.5.0'

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']
RETRY = 3

def init_log():
    logging.config.fileConfig('log.conf')


def import_basic_data(session):
    logging.info('Importing cities ...'.format())
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_city_postcode_from_xml
    work = hu_city_postcode_from_xml(session, 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/ZipCodes.xml',
                                     config.get_directory_cache_url())
    work.process()

    logging.info('Importing street types ...'.format())
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_street_types_from_xml
    work = hu_street_types_from_xml(session, 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/StreetTypes.xml',
                                    config.get_directory_cache_url())
    work.process()


def import_poi_data_module(module):
    try:
        db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                                  config.get_database_writer_password(),
                                                  config.get_database_writer_host(),
                                                  config.get_database_writer_port(),
                                                  config.get_database_poi_database()))
        mysql_pool = db.pool
        session_factory = sessionmaker(mysql_pool)
        Session = scoped_session(session_factory)
        session = Session()
        module = module.strip()
        logging.info('Processing {} module ...'.format(module))
        if module == 'hu_kh_bank':
            from osm_poi_matchmaker.dataproviders.hu_kh_bank import hu_kh_bank
            work = hu_kh_bank(session, config.get_directory_cache_url(), config.get_geo_prefer_osm_postcode(),
                              os.path.join(config.get_directory_cache_url(), 'hu_kh_bank.json'), 'K&H bank')
            insert_type(session, work.types())
            work.process()
            work = hu_kh_bank(session, config.get_directory_cache_url(), config.get_geo_prefer_osm_postcode(),
                              os.path.join(config.get_directory_cache_url(), 'hu_kh_atm.json'), 'K&H')
            work.process()
        elif module == 'hu_cib_bank':
            from osm_poi_matchmaker.dataproviders.hu_cib_bank import hu_cib_bank
            work = hu_cib_bank(session, '', os.path.join(config.get_directory_cache_url(), 'hu_cib_bank.html'),
                               config.get_geo_prefer_osm_postcode(), 'CIB Bank')
            insert_type(session, work.types())
            work = hu_cib_bank(session, '', os.path.join(config.get_directory_cache_url(), 'hu_cib_atm.html'),
                               config.get_geo_prefer_osm_postcode(), 'CIB Bank ATM')
        elif module == 'hu_posta_json':
            # Old code that uses JSON files
            from osm_poi_matchmaker.dataproviders.hu_posta_json import hu_posta_json
            # We only using csekkautomata since there is no XML from another data source
            work = hu_posta_json(session,
                                 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
                                 config.get_directory_cache_url(), 'hu_postacsekkautomata.json')
            work.process()
        else:
            mo = dataproviders_loader.import_module('osm_poi_matchmaker.dataproviders.{0}'.format(module), module)
            work = mo(session, config.get_directory_cache_url(), config.get_geo_prefer_osm_postcode())
            insert_type(session, work.types())
            work.process()
    except Exception as err:
        print(err)


def load_poi_data(database):
    logging.info('Loading POI_data from database ...')
    if not os.path.exists(config.get_directory_output()):
        os.makedirs(config.get_directory_output())
    # Build Dataframe from our POI database
    addr_data = database.query_all_gpd('poi_address')
    addr_data[['poi_addr_city', 'poi_postcode']] = addr_data[['poi_addr_city', 'poi_postcode']].fillna('0').astype(int)
    return addr_data


def load_common_data(database):
    logging.info('Loading common data from database ...')
    return database.query_all_pd('poi_common')


def export_raw_poi_data(addr_data, comm_data, postfix=''):
    logging.info('Exporting CSV files ...')
    # And merge and them into one Dataframe and save it to a CSV file
    save_csv_file(config.get_directory_output(), 'poi_common{}.csv'.format(postfix), comm_data, 'poi_common')
    save_csv_file(config.get_directory_output(), 'poi_address{}.csv'.format(postfix), addr_data, 'poi_address')


def export_raw_poi_data_xml(addr_data, postfix=''):
    with open(os.path.join(config.get_directory_output(), 'poi_address{}.osm'.format(postfix)), 'wb') as oxf:
        oxf.write(generate_osm_xml(addr_data))


def export_grouped_poi_data(data):
    # Generating CSV files group by poi_code
    output_dir = data[0]
    filename = data[1]
    rows = data[2]
    table = data[3]
    save_csv_file(output_dir, '{}.csv'.format(filename), rows, table)
    with open(os.path.join(output_dir, '{}.osm'.format(filename)), 'wb') as oxf:
        oxf.write(generate_osm_xml(rows))


def online_poi_matching(args):
    data, comm_data = args
    try:
        db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                                  config.get_database_writer_password(),
                                                  config.get_database_writer_host(),
                                                  config.get_database_writer_port(),
                                                  config.get_database_poi_database()))
        mysql_pool = db.pool
        session_factory = sessionmaker(mysql_pool)
        Session = scoped_session(session_factory)
        session = Session()
        osm_live_query = OsmApi()
        for i, row in data.iterrows():
            # for i, row in data[data['poi_code'].str.contains('tesco')].iterrows():
            common_row = comm_data.loc[comm_data['pc_id'] == row['poi_common_id']]
            # Try to search OSM POI with same type, and name contains poi_search_name within the specified distance
            if row['poi_search_name'] is not None and row['poi_search_name'] != '':
                # Try to search OSM POI with same type within the specified distance
                osm_query = (db.query_osm_shop_poi_gpd(row['poi_lon'], row['poi_lat'], common_row['poi_type'].item(),
                                                       row['poi_search_name']))
                if (row['poi_search_name'] is None or row['poi_search_name'] == '') or osm_query is None:
                    osm_query = (
                        db.query_osm_shop_poi_gpd(row['poi_lon'], row['poi_lat'], common_row['poi_type'].item()))
            # Enrich our data with OSM database POI metadata
            if osm_query is not None:
                # Collect additional OSM metadata. Note: this needs style change during osm2pgsql
                osm_id = osm_query['osm_id'].values[0]
                osm_node = osm_query['node'].values[0]
                # Set OSM POI coordinates for the node
                if osm_node == True:
                    data.at[i, 'poi_lat'] = osm_query['lat'].values[0]
                    data.at[i, 'poi_lon'] = osm_query['lon'].values[0]
                data.at[i, 'osm_id'] = osm_id
                data.at[i, 'osm_node'] = osm_node
                data.at[i, 'osm_version'] = osm_query['osm_version'].values[0]
                data.at[i, 'osm_changeset'] = osm_query['osm_changeset'].values[0]
                osm_timestamp = pd.to_datetime(str((osm_query['osm_timestamp'].values[0])))
                data.at[i, 'osm_timestamp'] = '{:{dfmt}T{tfmt}Z}'.format(osm_timestamp, dfmt='%Y-%m-%d',
                                                                            tfmt='%H:%M:%S')
                data.loc[[i], 'poi_distance'] = osm_query['distance'].values[0]
                # For OSM way also query node points
                if osm_node == False:
                    logging.info('This is an OSM way looking for id {} nodes.'.format(osm_id))
                    # Add list of nodes to the dataframe
                    nodes = db.query_ways_nodes(osm_id)
                    data.at[i, 'osm_nodes'] = nodes
                try:
                    # Download OSM POI way live tags
                    if osm_node == False:
                        for rtc in range(0, RETRY):
                            logging.info('Downloading OSM live tags to this way: {}.'.format(osm_id))
                            live_tags_container = osm_live_query.WayGet(osm_id)
                            if live_tags_container is not None:
                                data.at[i, 'osm_live_tags'] = live_tags_container['tag']
                                break
                            else:
                                logging.warning('Download of external data has failed.')
                    # Download OSM POI way live tags
                    else:
                        for rtc in range(0, RETRY):
                            logging.info('Downloading OSM live tags to this node: {}.'.format(osm_id))
                            live_tags_container = osm_live_query.NodeGet(osm_id)
                            if live_tags_container is not None:
                                data.at[i, 'osm_live_tags'] = live_tags_container['tag']
                                break
                            else:
                                logging.warning('Download of external data has failed.')
                except Exception as err:
                    logging.warning('There was an error during OSM request: {}.'.format(err))
                    traceback.print_exc()
        return data
    except Exception as err:
        print(err)


class WorkflowManager(object):

    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.queue = self.manager.Queue()
        self.NUMBER_OF_PROCESSES = multiprocessing.cpu_count()
        self.items = 0

    def start_poi_harvest(self):
        for m in config.get_dataproviders_modules_enable():
            self.queue.put(m)
        try:
            # Start multiprocessing in case multiple cores
            logging.info('Starting processing on {} cores.'.format(self.NUMBER_OF_PROCESSES))
            self.results = []
            self.pool = multiprocessing.Pool(processes=self.NUMBER_OF_PROCESSES)
            self.results = self.pool.map_async(import_poi_data_module, config.get_dataproviders_modules_enable())
            self.pool.close()
        except Exception as e:
            traceback.print_exc()
            print(e)

    def start_exporter(self, data, postfix=''):
        poi_codes = data['poi_code'].unique()
        modules = [[config.get_directory_output(), 'poi_address_{}{}'.format(postfix, c), data[data.poi_code == c],
                    'poi_address'] for c in poi_codes]
        try:
            # Start multiprocessing in case multiple cores
            logging.info('Starting processing on {} cores.'.format(self.NUMBER_OF_PROCESSES))
            self.results = []
            self.pool = multiprocessing.Pool(processes=self.NUMBER_OF_PROCESSES)
            self.results = self.pool.map_async(export_grouped_poi_data, modules)
            self.pool.close()
        except Exception as e:
            traceback.print_exc()
            print(e)

    def start_matcher(self, data, comm_data):
        try:
            workers = self.NUMBER_OF_PROCESSES
            self.pool = multiprocessing.Pool(processes=self.NUMBER_OF_PROCESSES)
            self.results = self.pool.map_async(online_poi_matching,
                                               [(d, comm_data) for d in np.array_split(data, workers)])
            self.pool.close()
            return pd.concat(list(self.results.get()))
        except Exception as e:
            traceback.print_exc()
            print(e)

    def join(self):
        self.pool.join()

def main():
    logging.info('Starting {0} ...'.format(__program__))
    db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                              config.get_database_writer_password(),
                                              config.get_database_writer_host(),
                                              config.get_database_writer_port(),
                                              config.get_database_poi_database()))
    mysql_pool = db.pool
    session_factory = sessionmaker(mysql_pool)
    Session = scoped_session(session_factory)
    session = Session()
    try:
        # import_basic_data(db.session)
        manager = WorkflowManager()
        manager.start_poi_harvest()
        manager.join()
        # Load basic dataset from database
        poi_addr_data = load_poi_data(db)
        # Download and load POI dataset to database
        poi_common_data = load_common_data(db)
        logging.info('Merging dataframes ...')
        poi_addr_data = pd.merge(poi_addr_data, poi_common_data, left_on='poi_common_id', right_on='pc_id', how='inner')
        # Add additional empty fields
        poi_addr_data['osm_id'] = None
        poi_addr_data['osm_node'] = None
        poi_addr_data['osm_version'] = None
        poi_addr_data['osm_changeset'] = None
        poi_addr_data['osm_timestamp'] = None
        poi_addr_data['osm_live_tags'] = None
        # Export non-transformed data
        export_raw_poi_data(poi_addr_data, poi_common_data)
        export_raw_poi_data_xml(poi_addr_data)
        logging.info('Saving poi_code grouped filesets...')
        # Export non-transformed filesets
        manager.start_exporter(poi_addr_data)
        manager.join()
        logging.info('Merging with OSM datasets ...')
        poi_addr_data['osm_nodes'] = None
        poi_addr_data['poi_distance'] = None
        # Enrich POI datasets from online OpenStreetMap database
        poi_addr_data = manager.start_matcher(poi_addr_data, poi_common_data)
        manager.join()
        # Export filesets
        export_raw_poi_data(poi_addr_data, poi_common_data, '_merge')
        manager.start_exporter(poi_addr_data, 'merge_')
        manager.join()

    except (KeyboardInterrupt, SystemExit):
        logging.info('Interrupt signal received')
        exit(1)
    except Exception as err:
        raise err

if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    init_log()
    timer = timing.Timing()
    main()
    logging.info('Total duration of process: {}. Finished, exiting and go home ...'.format(timer.end()))
