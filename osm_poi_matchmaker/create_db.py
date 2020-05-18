#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'kami911'
__program__ = 'create_db'
__version__ = '0.7.0'

try:
    import os
    import traceback
    import logging
    import logging.config
    import sys
    import numpy as np
    import pandas as pd
    import multiprocessing
    import copy
    from osm_poi_matchmaker.utils import config, timing
    from osm_poi_matchmaker.libs.osm import timestamp_now
    from osm_poi_matchmaker.libs.online_poi_matching import online_poi_matching
    from osm_poi_matchmaker.libs.import_poi_data_module import import_poi_data_module
    from osm_poi_matchmaker.libs.export import export_raw_poi_data, export_raw_poi_data_xml, export_grouped_poi_data, \
        export_grouped_poi_data_with_postcode_groups
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osm_poi_matchmaker.dao.poi_base import POIBase
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)

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


def load_poi_data(database):
    logging.info('Loading POI_data from database ...')
    if not os.path.exists(config.get_directory_output()):
        os.makedirs(config.get_directory_output())
    # Build Dataframe from our POI database
    addr_data = database.query_all_gpd_in_order('poi_address')
    addr_data[['poi_addr_city', 'poi_postcode']] = addr_data[['poi_addr_city', 'poi_postcode']].fillna('0').astype(int)
    return addr_data


def load_common_data(database):
    logging.info('Loading common data from database ...')
    return database.query_all_pd('poi_common')


class WorkflowManager(object):

    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.queue = self.manager.Queue()
        self.NUMBER_OF_PROCESSES = multiprocessing.cpu_count()
        self.items = 0
        self.pool = None
        self.results = []

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
            logging.error(e)
            logging.error(traceback.print_exc())


    def start_exporter(self, data: list, postfix: str = '', to_do = export_grouped_poi_data):
        poi_codes = data['poi_code'].unique()
        modules = [[config.get_directory_output(), 'poi_address_{}{}'.format(postfix, c), data[data.poi_code == c],
                    'poi_address'] for c in poi_codes]
        try:
            # Start multiprocessing in case multiple cores
            logging.info('Starting processing on {} cores.'.format(self.NUMBER_OF_PROCESSES))
            self.results = []
            self.pool = multiprocessing.Pool(processes=self.NUMBER_OF_PROCESSES)
            self.results = self.pool.map_async(to_do, modules)
            self.pool.close()
        except Exception as e:
            logging.error(e)
            logging.error(traceback.print_exc())


    def start_matcher(self, data, comm_data):
        try:
            workers = self.NUMBER_OF_PROCESSES
            self.pool = multiprocessing.Pool(processes=self.NUMBER_OF_PROCESSES)
            self.results = self.pool.map_async(online_poi_matching,
                                               [(d, comm_data) for d in np.array_split(data, workers)])
            self.pool.close()
            return pd.concat(list(self.results.get()), sort=False)
        except Exception as e:
            logging.error(e)
            logging.error(traceback.print_exc())

    def join(self):
        self.pool.join()


def main():
    logging.info('Starting {0} ...'.format(__program__))
    db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                              config.get_database_writer_password(),
                                              config.get_database_writer_host(),
                                              config.get_database_writer_port(),
                                              config.get_database_poi_database()))
    pgsql_pool = db.pool
    session_factory = sessionmaker(pgsql_pool)
    Session = scoped_session(session_factory)
    session = Session()
    try:
        import_basic_data(db.session)
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
        poi_addr_data['osm_timestamp'] = timestamp_now()
        poi_addr_data['osm_live_tags'] = None
        # Export non-transformed data
        export_raw_poi_data(poi_addr_data, poi_common_data)
        #export_raw_poi_data_xml(poi_addr_data)
        logging.info('Saving poi_code grouped filesets...')
        # Export non-transformed filesets
        manager.start_exporter(poi_addr_data)
        manager.join()
        logging.info('Merging with OSM datasets ...')
        poi_addr_data['osm_nodes'] = None
        poi_addr_data['poi_distance'] = None
        # Enrich POI datasets from online OpenStreetMap database
        logging.info('Starting online POI matching part...')
        poi_addr_data = manager.start_matcher(poi_addr_data, poi_common_data)
        manager.join()
        # Export filesets
        export_raw_poi_data(poi_addr_data, poi_common_data, '_merge')
        manager.start_exporter(poi_addr_data, 'merge_')
        manager.start_exporter(poi_addr_data, 'merge_', export_grouped_poi_data_with_postcode_groups)
        manager.join()

    except (KeyboardInterrupt, SystemExit):
        logging.info('Interrupt signal received')
        sys.exit(1)
    except Exception as err:
        raise err


if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    init_log()
    timer = timing.Timing()
    main()
    logging.info('Total duration of process: {}. Finished, exiting and go home ...'.format(timer.end()))
