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
    from osm_poi_matchmaker.libs.online_poi_matching import find_osm_matches, enrich_matched_pois, init_matcher_worker
    from osm_poi_matchmaker.libs.match_conflict_resolution import match_conflict_resolution
    from osm_poi_matchmaker.libs.import_poi_data_module import import_poi_data_module
    from osm_poi_matchmaker.libs.poi_patch import apply_poi_patches, load_poi_patches_from_db
    from osm_poi_matchmaker.libs.export import export_raw_poi_data, export_raw_poi_data_xml, \
        export_raw_poi_data_geojson, export_grouped_poi_data, \
        export_new_poi_data, export_existing_poi_data, \
        export_grouped_poi_data_new, export_grouped_poi_data_existing, \
        export_grouped_poi_data_with_postcode_groups
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from osm_poi_matchmaker.dao import poi_array_structure
    from osm_poi_matchmaker.libs.osm_prepare import index_osm_data
    from osm_poi_matchmaker.utils.memory_info import MemoryInfo
    from osm_poi_matchmaker.utils.log_context import ProviderLogFilter
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

RETRY = 3

POI_COLS = poi_array_structure.POI_DB
POI_COLS_RAW = poi_array_structure.POI_DB_RAW

PROCESS_DIVIDER = 1


def init_log():
    """Configure the logging subsystem from the 'log.conf' file in the current working directory.

    Also attaches ProviderLogFilter to the root logger, so every log line (including
    ones from shared library code called on a provider's behalf) can be tagged with
    the data provider currently being harvested - see log.conf's %(provider)s and
    utils/log_context.py. This must happen before start_poi_harvest() creates its
    multiprocessing.Pool, so forked workers inherit the filter."""
    logging.config.fileConfig('log.conf')
    logging.getLogger().addFilter(ProviderLogFilter())


def import_basic_data(session):
    """Import the static reference datasets (patches, countries, cities, street types) needed before harvesting.

    Runs once at the start of the pipeline (STAGE 0) to populate lookup tables from local
    TSV files (poi_patch.tsv, country.tsv) and remote Hungarian Post XML feeds (zip codes,
    street types) via the hu_generic dataproviders.

    Args:
        session: SQLAlchemy session used by the underlying dataprovider import jobs.
    """
    logging.info('Importing patch table…')
    from osm_poi_matchmaker.dataproviders.hu_generic import poi_patch_from_csv
    work = poi_patch_from_csv(session, 'poi_patch.tsv')
    work.process()

    logging.info('Importing countries…')
    from osm_poi_matchmaker.dataproviders.hu_generic import poi_country_from_csv
    work = poi_country_from_csv(session, 'country.tsv')
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
    """Load a POI table from the database into a DataFrame and normalize its columns.

    Ensures the configured output/cache directories exist, then queries the table and
    assigns the appropriate column names (raw vs. processed schema). Also collapses
    NaN values in poi_addr_city/poi_postcode to None, since downstream code expects
    None rather than NaN for "missing".

    Args:
        database: POIBase (or compatible) database wrapper exposing query_all_gpd_in_order().
        table (str): Name of the table to load. Defaults to 'poi_address_raw'.
        raw (bool): If True, use the raw-schema column names (POI_COLS_RAW); otherwise use
            the processed-schema column names (POI_COLS). Defaults to True.

    Returns:
        pandas.DataFrame: The loaded POI data with normalized columns.
    """
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
    """Load the 'poi_common' table (shared POI type/tag definitions) from the database.

    Args:
        database: POIBase (or compatible) database wrapper exposing query_all_pd().

    Returns:
        pandas.DataFrame: The poi_common table contents.
    """
    logging.info('Loading common data from database…')
    return database.query_all_pd('poi_common')


class WorkflowManager(object):
    """Owns the multiprocessing.Pool used to run each pipeline phase (harvest, export, matcher) in parallel.

    Only one phase runs at a time: each start_*() method creates a fresh pool sized to the
    host's CPU count (divided by process_divider), dispatches work with map_async(), waits
    for the results via _wait_for_results(), and tears the pool down again. join() can be
    used to force an early pool shutdown between phases. Also accumulates per-phase timing
    and statistics (harvest_stats, matcher_stats) for the final log_summary() report.
    """

    def __init__(self):
        """Initialize the manager, its shared Queue, and empty pool/stats state."""
        self.manager = multiprocessing.Manager()
        self.queue = self.manager.Queue()
        self.NUMBER_OF_PROCESSES = multiprocessing.cpu_count()
        self.pool = None
        self.results = None
        self.harvest_stats = []
        self.harvest_duration = None
        self.matcher_stats = {}
        self.matcher_duration = None
        self.conflict_stats = {}
        self.conflict_duration = None

    def _create_pool(self, process_divider=PROCESS_DIVIDER, initializer=None):
        """Create a new multiprocessing.Pool, closing any pre-existing pool first.

        Args:
            process_divider (int): Divides the host's CPU count to determine the pool size
                (at least 1 process). Defaults to PROCESS_DIVIDER (1, i.e. use all CPUs).
            initializer (callable, optional): Function run once in each worker process on
                startup, e.g. init_matcher_worker to set up a per-worker DB connection.
        """
        if self.pool is not None:
            logging.warning('Existing pool found, closing it first.')
            self.pool.close()
            self.pool.join()
            logging.info('Old pool closed.')
        process_count = (max(1, self.NUMBER_OF_PROCESSES // process_divider))
        logging.info('Creating new multiprocessing pool with %d processes.', process_count)
        self.pool = multiprocessing.Pool(processes=process_count, initializer=initializer)

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
        """Close and join the current pool (if any), then clear self.pool. Never raises."""
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
        """Run every enabled dataprovider module in parallel (STAGE 2) and collect their harvest stats.

        Creates a pool, dispatches config.get_dataproviders_modules_enable() to
        import_poi_data_module() via map_async(), and stores the returned per-module
        stats dicts in self.harvest_stats. Exceptions are logged, not re-raised, so a
        harvest failure leaves harvest_stats as an empty list rather than aborting main().
        """
        phase_timer = timing.Timing()
        try:
            logging.info('Starting processing POI harvest.')
            self._create_pool()
            # Start multiprocessing in case multiple cores
            # process_count = 1
            self.results = self.pool.map_async(import_poi_data_module, config.get_dataproviders_modules_enable(),)
            # chunksize=100)
            self.harvest_stats = self._wait_for_results('POI harvest', return_results=True) or []
            logging.info('Finished processing POI harvest.')
        except Exception as e:
            logging.exception('Exception occurred', exc_info=True)
        finally:
            self.harvest_duration = phase_timer.end()

    def start_exporter(self, data: pd.DataFrame, postfix: str = '', to_do=export_grouped_poi_data,
                       infix: str = ''):
        """Export data in parallel, split into one job per distinct poi_code (STAGE 8/10/11).

        Args:
            data (pandas.DataFrame): POI data to export; must contain a 'poi_code' column.
            postfix (str): Suffix appended to output filenames (e.g. 'merge_'). Defaults to ''.
            to_do (callable): Worker function each pool process runs on one poi_code's slice
                of data. Defaults to export_grouped_poi_data.
            infix (str): Additional filename segment inserted between postfix and poi_code
                (e.g. 'new_', 'existing_'). Defaults to ''.
        """
        logging.debug(data.to_string())
        logging.info('Preparing export jobs…')
        poi_codes = data['poi_code'].unique()
        modules = [[config.get_directory_output(), f'poi_address_{postfix}{infix}{c}', data[data.poi_code == c],
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
        """Find OSM match candidates for harvested POIs in parallel (STAGE 9 phase 1)
        and aggregate the results.

        Splits data into NUMBER_OF_PROCESSES * 8 chunks, runs find_osm_matches() on each
        chunk via a pool created with the per-worker init_matcher_worker initializer (so each
        worker reuses one DB connection instead of opening one per chunk), then concatenates
        the returned chunks back into a single DataFrame. Deliberately does NOT touch the
        live OSM API - only the local PostGIS DB - so match_conflict_resolution() can settle
        the final osm_id<->POI assignment on the *whole* dataset before any row does that
        (expensive, network-bound) work; see start_enricher() for that second pass.

        Args:
            data (pandas.DataFrame): POI data to match, split row-wise across worker processes.
            comm_data (pandas.DataFrame): Shared poi_common reference data passed to every worker.

        Returns:
            pandas.DataFrame: The candidate-matched POI data (all chunks concatenated), or None
            if an exception occurred before the result could be assembled.
        """
        phase_timer = timing.Timing()
        try:
            # Start multiprocessing in case multiple cores
            logging.info('Starting processing matcher.')
            self._create_pool(initializer=init_matcher_worker)
            idx_chunks = np.array_split(np.arange(len(data)), self.NUMBER_OF_PROCESSES * 8)
            split_data = [data.iloc[idx] for idx in idx_chunks]
            logging.info('Starting matcher on %d data chunks.', len(split_data))
            self.results = self.pool.map_async(find_osm_matches, [(chunk, comm_data) for chunk in split_data],
                                               chunksize=16)
            result_chunks = self._wait_for_results('matcher', return_results=True, timeout=360000)
            combined_result = pd.concat(result_chunks, ignore_index=True, sort=False)
            return combined_result
        except Exception as e:
            logging.exception('Exception occurred', exc_info=True)
        finally:
            self.matcher_duration = phase_timer.end()

    def start_enricher(self, data: pd.DataFrame, comm_data: pd.DataFrame):
        """Download live OSM data for the *final* (post-conflict-resolution) matches, and
        finalize genuinely-new POIs, in parallel (STAGE 9 phase 2).

        Same chunking/pooling pattern as start_matcher() (reuses init_matcher_worker), but
        runs enrich_matched_pois() instead - see its docstring. Must only be called after
        match_conflict_resolution() has run on `data`, so a POI demoted there gets the "new
        POI" treatment here instead of a live tag download for a match it no longer has.
        Computes the final matcher_stats (new/matched/errors), overwriting whatever
        start_matcher() computed before conflict resolution - poi_new only reflects the
        final state once this phase (and match_conflict_resolution() before it) has run.

        Args:
            data (pandas.DataFrame): Matched-and-deduplicated POI data from
                match_conflict_resolution(), split row-wise across worker processes.
            comm_data (pandas.DataFrame): Shared poi_common reference data (kept for
                interface symmetry with start_matcher(); enrich_matched_pois() doesn't use it).

        Returns:
            pandas.DataFrame: The enriched POI data (all chunks concatenated), or None if an
            exception occurred before the result could be assembled.
        """
        phase_timer = timing.Timing()
        try:
            logging.info('Starting processing enricher.')
            self._create_pool(initializer=init_matcher_worker)
            idx_chunks = np.array_split(np.arange(len(data)), self.NUMBER_OF_PROCESSES * 8)
            split_data = [data.iloc[idx] for idx in idx_chunks]
            logging.info('Starting enricher on %d data chunks.', len(split_data))
            self.results = self.pool.map_async(enrich_matched_pois, [(chunk, comm_data) for chunk in split_data],
                                               chunksize=16)
            result_chunks = self._wait_for_results('enricher', return_results=True, timeout=360000)
            combined_result = pd.concat(result_chunks, ignore_index=True, sort=False)
            self.matcher_stats = {
                'total': int(len(combined_result)),
                'new': int((combined_result['poi_new'] == True).sum()) if 'poi_new' in combined_result else 0,
                'matched': int((combined_result['poi_new'] == False).sum()) if 'poi_new' in combined_result else 0,
                'errors': int((combined_result['match_error'] == True).sum()) if 'match_error' in combined_result else 0,
            }
            return combined_result
        except Exception as e:
            logging.exception('Exception occurred', exc_info=True)
        finally:
            self.matcher_duration = phase_timer.end()

    def start_conflict_resolver(self, data: pd.DataFrame):
        """Resolve OSM ID conflicts where multiple POIs match the same OSM element (STAGE 9).

        After online_poi_matching, some POIs may have been matched to the same OSM element.
        This stage iteratively reassigns conflicting POIs to None (removing their OSM match)
        until each OSM element has at most one associated POI.

        Args:
            data (pandas.DataFrame): Matched POI data from start_matcher().

        Returns:
            pandas.DataFrame: POI data with OSM ID conflicts resolved.
        """
        phase_timer = timing.Timing()
        try:
            logging.info('Starting conflict resolution.')
            resolved_data, stats = match_conflict_resolution(data)
            logging.info('Conflict resolution complete: %d initial conflicts, %d resolved in %d iterations, %d unresolved',
                        stats['initial_conflicts'], stats['resolved'], stats['iterations'], stats['unresolved'])
            self.conflict_stats = stats
            return resolved_data
        except Exception as e:
            logging.exception('Exception occurred during conflict resolution', exc_info=True)
            return data
        finally:
            self.conflict_duration = phase_timer.end()

    def log_summary(self):
        """Log a final per-provider / per-phase statistics summary for this run."""
        lines = ['==== Import statistics ====']
        if self.harvest_stats:
            lines.append('-- STAGE 2: POI harvesting (duration: %s) --' % self.harvest_duration)
            lines.append('{:<28} {:>10} {:>10} {:>10} {:>10}'.format(
                'Provider', 'Harvested', 'HarvErr', 'DB-Insert', 'DB-Err'))
            total = {'harvested': 0, 'harvest_errors': 0, 'db_inserted': 0, 'db_errors': 0}
            failed_modules = []
            for entry in sorted(self.harvest_stats, key=lambda e: e.get('module', '')):
                if 'error' in entry:
                    failed_modules.append(entry['module'])
                    continue
                lines.append('{:<28} {:>10} {:>10} {:>10} {:>10}'.format(
                    entry.get('module', '?'), entry.get('harvested', 0), entry.get('harvest_errors', 0),
                    entry.get('db_inserted', 0), entry.get('db_errors', 0)))
                for key in total:
                    total[key] += entry.get(key, 0)
            lines.append('{:<28} {:>10} {:>10} {:>10} {:>10}'.format(
                'TOTAL', total['harvested'], total['harvest_errors'], total['db_inserted'], total['db_errors']))
            if failed_modules:
                lines.append('Completely failed modules: %s' % ', '.join(failed_modules))
        if self.matcher_stats:
            lines.append('-- STAGE 8: Online POI matching (duration: %s) --' % self.matcher_duration)
            lines.append('Total: {total}   New: {new}   Existing: {matched}   Errors: {errors}'.format(
                **self.matcher_stats))
        lines.append('=============================')
        logging.info('\n'.join(lines))

    def join(self):
        """Force an early join/shutdown of the current pool, if one is still active."""
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
    """Run the full POI import pipeline end to end (STAGE 0 through STAGE 11).

    Connects to the database, then sequentially: imports basic reference data, indexes
    OSM data, harvests POIs from all dataproviders, loads and merges the harvested/common
    data, applies patch overrides, adds OSM metadata fields, exports raw data, runs the
    online OSM matcher, and exports the matched/grouped result sets. Logs a final summary
    via WorkflowManager.log_summary() on success.

    Returns:
        int: 0 on success, 1 if interrupted (KeyboardInterrupt/SystemExit), 2 if an
        unhandled exception occurred during the pipeline.
    """
    logging.info('Starting %s …', __program__)
    mem_info = MemoryInfo()

    # --- Database connection initialization ---
    db = POIBase('{}://{}:{}@{}:{}/{}'.format(
        config.get_database_type(),
        config.get_database_writer_username(),
        config.get_database_writer_password(),
        config.get_database_writer_host(),
        config.get_database_writer_port(),
        config.get_database_poi_database()
    ))

    pgsql_pool = db.pool
    session_factory = sessionmaker(bind=pgsql_pool)
    session_object = scoped_session(session_factory)

    try:
        session = session_object()
        # --- STAGE 0 ---
        logging.info('Starting STAGE 0 – Importing basic datasets from external databases.')
        import_basic_data(session)
        mem_info.log_top_memory_snapshot('STAGE 0')

        # --- STAGE 1 ---
        logging.info('Starting STAGE 1 – Adding index for database.')
        index_osm_data(session)
        mem_info.log_top_memory_snapshot('STAGE 1')

        # --- STAGE 2 ---
        logging.info('Starting STAGE 2 – POI harvesting from external sites and files.')
        manager = WorkflowManager()
        manager.start_poi_harvest()
        manager.join()
        logging.info("STAGE 2 – POI harvesting has finished successfully.")
        mem_info.log_top_memory_snapshot('STAGE 2')

        # --- STAGE 3 ---
        logging.info('Starting STAGE 3 – Loading persisted data into memory.')
        poi_addr_data = load_poi_data(db, 'poi_address_raw', True)
        mem_info.log_top_memory_snapshot('STAGE 3')

        # --- STAGE 4 ---
        logging.info('Starting STAGE 4 – Loading common data into memory.')
        poi_common_data = load_common_data(db)
        mem_info.log_top_memory_snapshot('STAGE 4')

        # --- STAGE 5 ---
        logging.info('Starting STAGE 5 – Merging and preparing dataframe.')
        poi_addr_data = pd.merge(
            poi_addr_data, poi_common_data,
            left_on='poi_common_id', right_on='pc_id', how='inner'
        )
        mem_info.log_top_memory_snapshot('STAGE 5')

        # --- STAGE 6 ---
        logging.info('Starting STAGE 6 – Applying poi_patch address overrides.')
        patch_df = load_poi_patches_from_db(db)
        poi_addr_data = apply_poi_patches(poi_addr_data, patch_df)
        logging.info("STAGE 6 – POI address patching has finished successfully.")
        mem_info.log_top_memory_snapshot('STAGE 6')

        # --- STAGE 7 ---
        logging.info('Starting STAGE 7 – Adding OpenStreetMap metadata fields.')
        # New fields for OpenStreetMap data
        now = datetime.datetime.now(datetime.UTC)
        poi_addr_data['osm_id'] = None
        poi_addr_data['osm_node'] = None
        poi_addr_data['osm_version'] = None
        poi_addr_data['osm_changeset'] = None
        poi_addr_data['osm_timestamp'] = now
        poi_addr_data['osm_live_tags'] = None
        logging.info("STAGE 7 – POI dataframe merging has finished successfully.")

        # --- STAGE 8 ---
        logging.info('Starting STAGE 8 – Exporting.')
        export_raw_poi_data(poi_addr_data, poi_common_data)
        export_raw_poi_data_xml(poi_addr_data)
        export_raw_poi_data_geojson(poi_addr_data)
        logging.info('Saving POI code grouped filesets…')
        manager.start_exporter(poi_addr_data)
        manager.join()
        logging.info("STAGE 8 – Exporting has finished successfully.")
        mem_info.log_top_memory_snapshot('STAGE 8')

        # --- STAGE 9 ---
        logging.info('Starting STAGE 9 – Online POI matching and conflict resolution.')
        # Phase 1: find candidate osm_id matches (local DB only, no live OSM API
        # calls yet) for the whole dataset.
        poi_addr_data = manager.start_matcher(poi_addr_data, poi_common_data)
        manager.join()
        if poi_addr_data is None:
            raise RuntimeError('STAGE 9 – Online POI matching failed, aborting pipeline.')
        logging.info("STAGE 9 – Candidate OSM matching finished successfully.")

        # Phase 1.5: resolve (osm_id, poi_type) conflicts on the *whole* dataset,
        # before any row does the expensive live-tag download below - so a POI that
        # loses a conflict never wastes an API call, and never carries live tag
        # data from a match it no longer has.
        logging.info('Resolving OSM ID conflicts.')
        try:
            poi_addr_data, conflict_stats = match_conflict_resolution(poi_addr_data)
            logging.info('Conflict resolution: %d initial conflicts, %d resolved in %d iterations, %d unresolved',
                        conflict_stats['initial_conflicts'], conflict_stats['resolved'],
                        conflict_stats['iterations'], conflict_stats['unresolved'])
        except Exception as e:
            logging.error('Conflict resolution failed: %s', e, exc_info=True)
            raise

        # Phase 2: download live OSM data for the *final* matches only, and
        # finalize genuinely-new (including conflict-demoted) POIs.
        poi_addr_data = manager.start_enricher(poi_addr_data, poi_common_data)
        manager.join()
        if poi_addr_data is None:
            raise RuntimeError('STAGE 9 – OSM enrichment failed, aborting pipeline.')
        logging.info("STAGE 9 – Online POI matching and conflict resolution finished successfully.")
        mem_info.log_top_memory_snapshot('STAGE 9')

        # insert_poi_dataframe(session, poi_addr_data, False)

        # --- STAGE 10 ---
        logging.info('Starting STAGE 10 – Exporting matched POI.')
        prefix = 'merge_'
        export_raw_poi_data(poi_addr_data, poi_common_data, prefix)
        export_raw_poi_data_geojson(poi_addr_data, prefix)
        export_new_poi_data(poi_addr_data, prefix)
        export_existing_poi_data(poi_addr_data, prefix)

        manager.start_exporter(poi_addr_data, prefix)
        manager.start_exporter(poi_addr_data, prefix, export_grouped_poi_data_new, infix='new_')
        manager.start_exporter(poi_addr_data, prefix, export_grouped_poi_data_existing, infix='existing_')
        manager.join()
        logging.info("STAGE 10 – Matched POI exported successfully.")
        mem_info.log_top_memory_snapshot('STAGE 10')

        # --- STAGE 11 ---
        logging.info('Starting STAGE 11 – Exporting grouped matched POI.')
        manager.start_exporter(poi_addr_data, prefix, export_grouped_poi_data_with_postcode_groups)
        manager.join()
        logging.info("STAGE 11 – Grouped POI exported successfully.")
        mem_info.log_top_memory_snapshot('STAGE 11')

        manager.log_summary()

        logging.info('%s finished successfully.', __program__)
        return 0

    except (KeyboardInterrupt, SystemExit):
        logging.info('Interrupt signal received. Exiting gracefully.')
        return 1

    except Exception:
        logging.exception('Critical error occurred during pipeline execution.')
        return 2

    finally:
        session_object.remove()


if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    init_log()
    timer = timing.Timing()
    exit_code = main()
    logging.info('Total duration of process: %s. Finished, exiting…', timer.end())
    sys.exit(exit_code)
