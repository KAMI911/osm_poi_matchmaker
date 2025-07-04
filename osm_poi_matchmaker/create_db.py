#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'kami911'
__program__ = 'create_db'
__version__ = '0.7.0'

try:
    import os
    import logging
    import logging.config
    import sys
    import numpy as np
    import pandas as pd
    import multiprocessing
    import datetime
    import traceback
    from osm_poi_matchmaker.utils import config, timing
    from osm_poi_matchmaker.libs.osm import timestamp_now
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.online_poi_matching import online_poi_matching
    from osm_poi_matchmaker.libs.import_poi_data_module import import_poi_data_module
    from osm_poi_matchmaker.libs.export import export_raw_poi_data, export_raw_poi_data_xml, export_grouped_poi_data, \
        export_grouped_poi_data_with_postcode_groups
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from osm_poi_matchmaker.dao import poi_array_structure
    from osm_poi_matchmaker.libs.osm_prepare import index_osm_data
    from utils.memory_info import MemoryInfo
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

RETRY = 3

POI_COLS = poi_array_structure.POI_DB
POI_COLS_RAW = poi_array_structure.POI_DB_RAW

PROCESS_DIVIDER = 2


def init_log():
    logging.config.fileConfig('log.conf')


def import_basic_data(session):
    logging.info('Importing patch table…')
    from osm_poi_matchmaker.dataproviders.hu_generic import poi_patch_from_csv
    work = poi_patch_from_csv(session, 'poi_patch.csv')
    work.process()

    logging.info('Importing countries…')
    from osm_poi_matchmaker.dataproviders.hu_generic import poi_country_from_csv
    work = poi_country_from_csv(session, 'country.csv')
    work.process()

    logging.info('Importing cities…')
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_city_postcode_from_xml
    work = hu_city_postcode_from_xml(session, 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/ZipCodes.xml',
                                     config.get_directory_cache_url())
    logging.info('Processing cities…')
    work.process()

    logging.info('Importing street types…')
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_street_types_from_xml
    work = hu_street_types_from_xml(session, 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/StreetTypes.xml',
                                    config.get_directory_cache_url())
    work.process()


def load_poi_data(database, table='poi_address_raw', raw=True):
    logging.info('Loading {} table from database…'.format(table))
    if not os.path.exists(config.get_directory_output()):
        os.makedirs(config.get_directory_output())
    if not os.path.exists(os.path.join(config.get_directory_cache_url(), 'cache')):
        os.makedirs(os.path.join(config.get_directory_cache_url(), 'cache'))
    # Build Dataframe from our POI database
    addr_data = database.query_all_gpd_in_order(table)
    if raw is True:
        addr_data.columns = POI_COLS_RAW
    else:
        addr_data.columns = POI_COLS
    addr_data[['poi_addr_city', 'poi_postcode']] = addr_data[['poi_addr_city', 'poi_postcode']].astype('str').\
        fillna(np.nan).replace([np.nan], [None])

    return addr_data


def load_common_data(database):
    logging.info('Loading common data from database…')
    return database.query_all_pd('poi_common')


class WorkflowManager(object):

    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.queue = self.manager.Queue()
        self.NUMBER_OF_PROCESSES = multiprocessing.cpu_count()
        self.pool = None
        self.results = None

    def _create_pool(self, process_divider=PROCESS_DIVIDER):
        if self.pool is not None:
            logging.warning('Existing pool found, closing it first.')
            self.pool.close()
            self.pool.join()
            logging.info('Old pool closed.')
        process_count = (max(1, self.NUMBER_OF_PROCESSES // process_divider))
        logging.info('Creating new multiprocessing pool with %d processes.', process_count)
        self.pool = multiprocessing.Pool(processes=process_count)

    def _wait_for_results(self, task_name: str, return_results=False, timeout=36000):
        """ Wait for async map to finish and handle errors."""
        try:
            logging.info('Waiting for %s results (timeout %d sec)…', task_name, timeout)
            results = self.results.get(timeout=timeout)
            logging.info('%s completed successfully.', task_name)
            return results if return_results else None
        except multiprocessing.TimeoutError:
            logging.error('%s timed out after %d seconds.', task_name, timeout)
            raise
        except Exception as e:
            logging.exception('Exception in %s: %s', task_name, e)
            raise
        finally:
            self._cleanup_pool()

    def _cleanup_pool(self):
        if self.pool is not None:
            try:
                self.pool.close()
                self.pool.join()
                logging.info('Pool cleaned up.')
            except Exception as e:
                logging.warning('Exception during pool cleanup: %s', e)
            finally:
                self.pool = None

    def start_poi_harvest(self):
        try:
            logging.info('Starting processing POI harvest.')
            self._create_pool()
            # Start multiprocessing in case multiple cores
            # process_count = 1
            self.results = self.pool.map_async(import_poi_data_module, config.get_dataproviders_modules_enable(),)
            # chunksize=100)
            self._wait_for_results('POI harvest')
            logging.info('Finished processing POI harvest.')
        except Exception as e:
            logging.exception('Exception occurred', exc_info=True)

    def start_exporter(self, data: pd.DataFrame, postfix: str = '', to_do=export_grouped_poi_data):
        logging.debug(data.to_string())
        logging.info('Preparing export jobs…')
        poi_codes = data['poi_code'].unique()
        modules = [[config.get_directory_output(), f'poi_address_{postfix}{c}', data[data.poi_code == c],
                    'poi_address'] for c in poi_codes]
        try:
            logging.info('Starting processing export.')
            self._create_pool()
            logging.info('Starting export with %d export groups.', len(modules))
            self.results = self.pool.map_async(to_do, modules)  # chunksize=100)
            self._wait_for_results('exporter', timeout=360000)
            logging.info('Finished processing export.')
        except Exception as e:
            logging.exception('Exception occurred', exc_info=True)

    def start_matcher(self, data: pd.DataFrame, comm_data: pd.DataFrame):
        try:
            # Start multiprocessing in case multiple cores
            logging.info('Starting processing matcher.')
            self._create_pool()
            if len(data) > self.NUMBER_OF_PROCESSES * 8:
                split_data = np.array_split(data, self.NUMBER_OF_PROCESSES * 8)
            else:
                split_data = np.array_split(data, self.NUMBER_OF_PROCESSES * 8)
            logging.info('Starting matcher on %d data chunks.', len(split_data))
            self.results = self.pool.map_async(online_poi_matching, [(chunk, comm_data) for chunk in split_data],
                                               chunksize=16)
            result_chunks = self._wait_for_results('matcher', return_results=True, timeout=360000)
            combined_result = pd.concat(result_chunks, ignore_index=True, sort=False)
            return combined_result
        except Exception as e:
            logging.exception('Exception occurred', exc_info=True)

    def join(self):
        if self.pool is not None:
            try:
                self.pool.join()
                logging.info('Pool joined manually.')
            except Exception as e:
                logging.warning('Exception during manual join: %s', e)
            finally:
                self.pool = None
        else:
            logging.warning('No active pool to join.')

def main():
    logging.info('Starting %s …', __program__)
    mem_info = MemoryInfo()
    db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                              config.get_database_writer_password(),
                                              config.get_database_writer_host(),
                                              config.get_database_writer_port(),
                                              config.get_database_poi_database()))
    pgsql_pool = db.pool
    session_factory = sessionmaker(pgsql_pool)
    session_object = scoped_session(session_factory)
    try:
        logging.info('Starting STAGE 0 – Importing basic datasets from external databases.')
        import_basic_data(session_object())
        mem_info.log_top_memory_snapshot('STAGE 0')
        logging.info('Starting STAGE 1 – Adding index for database.')
        index_osm_data(session_object())
        mem_info.log_top_memory_snapshot('STAGE 1')
        logging.info('Starting STAGE 2 – Do POI harversting from external sites and files.')
        manager = WorkflowManager()
        manager.start_poi_harvest()
        manager.join()
        mem_info.log_top_memory_snapshot('STAGE 2')
        logging.info('Starting STAGE 3 – Loading database persisted data into memory.')
        # Load basic dataset from database
        poi_addr_data = load_poi_data(db, 'poi_address_raw', True)
        mem_info.log_top_memory_snapshot('STAGE 3')
        # Download and load POI dataset to database
        logging.info('Starting STAGE 4 – Merge all available information in memory.')
        poi_common_data = load_common_data(db)
        logging.info('Merging dataframes …')
        poi_addr_data = pd.merge(poi_addr_data, poi_common_data, left_on='poi_common_id', right_on='pc_id', how='inner')
        mem_info.log_top_memory_snapshot('STAGE 4')
        # Add additional empty fields
        logging.info('Starting STAGE 5 – Dropping unnecessary data from memory.')
        del poi_addr_data
        mem_info.log_top_memory_snapshot('STAGE 5')
        logging.info('Starting STAGE 6…')
        poi_addr_data = load_poi_data(db, 'poi_address_raw', True)
        logging.info('Merging dataframes…')
        poi_addr_data = pd.merge(poi_addr_data, poi_common_data, left_on='poi_common_id', right_on='pc_id', how='inner')
        poi_addr_data['osm_id'] = None
        poi_addr_data['osm_node'] = None
        poi_addr_data['osm_version'] = None
        poi_addr_data['osm_changeset'] = None
        poi_addr_data['osm_timestamp'] = datetime.datetime.now()
        poi_addr_data['osm_live_tags'] = None
        # Export non-transformed data
        export_raw_poi_data(poi_addr_data, poi_common_data)
        export_raw_poi_data_xml(poi_addr_data)
        logging.info('Saving poi_code grouped filesets…')
        # Export non-transformed filesets
        manager.start_exporter(poi_addr_data)
        manager.join()
        logging.info('Merging with OSM datasets…')
        poi_addr_data['osm_nodes'] = None
        poi_addr_data['poi_distance'] = None
        mem_info.log_top_memory_snapshot('STAGE 6')
        logging.info('Starting STAGE 7 – online POI matching…')
        # Enrich POI datasets from online OpenStreetMap database
        logging.info('Starting online POI matching part…')
        poi_addr_data = manager.start_matcher(poi_addr_data, poi_common_data)
        manager.join()
        '''
        poi_addr_data['geometry_wkb'] = poi_addr_data['poi_geom'].apply(lambda poi_geom: poi_geom.wkb)
        insert_poi_dataframe(session, poi_addr_data, False)
        '''
        # Export filesets
        prefix = 'merge_'
        export_raw_poi_data(poi_addr_data, poi_common_data, prefix)
        mem_info.log_top_memory_snapshot('STAGE 7')
        logging.info('Starting STAGE 8 – Exporting matched POI.')
        manager.start_exporter(poi_addr_data, prefix)
        manager.join()
        mem_info.log_top_memory_snapshot('STAGE 8')
        logging.info('Starting STAGE 9 – Exporting grouped matched POI.')
        manager.start_exporter(poi_addr_data, prefix, export_grouped_poi_data_with_postcode_groups)
        manager.join()
        mem_info.log_top_memory_snapshot('STAGE 9')
    except (KeyboardInterrupt, SystemExit):
        logging.info('Interrupt signal received')
        sys.exit(1)
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())


if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    init_log()
    timer = timing.Timing()
    main()
    logging.info('Total duration of process: %s. Finished, exiting…', timer.end())
