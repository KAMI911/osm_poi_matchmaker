# -*- coding: utf-8 -*-

__author__ = 'kami911'

try:
    import logging
    import sys
    import os
    import traceback
    from sqlalchemy.orm import scoped_session, sessionmaker
    from sqlalchemy import inspect, text
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from osm_poi_matchmaker.dao.data_structure import POI_OSM_cache, POI_address, POI_address_raw, POI_common, POI_osm,\
        POI_patch
    from osm_poi_matchmaker.utils import config, dataproviders_loader
    from osm_poi_matchmaker.dao.data_handlers import insert_type, get_or_create
    from osm_poi_matchmaker.utils.log_context import set_current_provider, clear_current_provider
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def import_poi_data_module(module: str) -> dict:
    """Process all data provider modules enabled in app.conf and write to the database

    Args:
        module (str): Name of module to run

    Returns:
        dict: Per-module stats: {'module': str, 'harvested': int, 'harvest_errors': int,
              'db_inserted': int, 'db_errors': int} or {'module': str, 'error': str} if the
              module failed outright before/without producing any stats.
    """
    stats = {'harvested': 0, 'harvest_errors': 0, 'db_inserted': 0, 'db_errors': 0}

    def _add(one: dict):
        """Accumulate one sub-call's stats dict into the outer `stats` total (used
        for the special-cased providers below that run process() more than once,
        e.g. hu_kh_bank's separate bank/ATM passes)."""
        for key in stats:
            stats[key] += one.get(key, 0)

    try:
        set_current_provider(module.strip())
        db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                                  config.get_database_writer_password(),
                                                  config.get_database_writer_host(),
                                                  config.get_database_writer_port(),
                                                  config.get_database_poi_database()))
        if config.get_database_start_drop_poi_tables():
            delete_poi_tables(db)
        pgsql_pool = db.pool
        session_factory = sessionmaker(pgsql_pool)
        session_object = scoped_session(session_factory)
        one_session = session_object()
        module = module.strip()
        logging.info('Processing %s module ...', module)
        if module == 'hu_kh_bank':
            from osm_poi_matchmaker.dataproviders.hu_kh_bank import hu_kh_bank
            work = hu_kh_bank(session_object(), config.get_directory_cache_url(), True,
                              os.path.join(config.get_directory_cache_url(), 'hu_kh_bank.json'), 'K&H Bank')
            insert_type(session_object(), work.types())
            _add(work.process() or {})
            work = hu_kh_bank(session_object(), config.get_directory_cache_url(), True,
                              os.path.join(config.get_directory_cache_url(), 'hu_kh_atm.json'), 'K&H Bank ATM')
            _add(work.process() or {})
        elif module == 'hu_cib_bank':
            from osm_poi_matchmaker.dataproviders.hu_cib_bank import hu_cib_bank
            # Same endpoint the map widget on https://www.cib.hu/Maganszemelyek/leave-message/branch.html
            # calls (POST body's locationType picks BRANCH vs ATM - see hu_cib_bank.process()).
            cib_locator_url = ('https://www.cib.hu/digitalServicesServlet/?operation=searchLocations'
                               '&headers=lbsHeader&endpointName=searchLocations&locale=en&bank=CIB')
            work = hu_cib_bank(session_object(), config.get_directory_cache_url(), True,
                               cib_locator_url, 'CIB Bank')
            insert_type(session_object(), work.types())
            _add(work.process() or {})
            work = hu_cib_bank(session_object(), config.get_directory_cache_url(), True,
                               cib_locator_url, 'CIB Bank ATM')
            _add(work.process() or {})
        elif module == 'hu_posta_json':
            # Old code that uses JSON files
            from osm_poi_matchmaker.dataproviders.hu_posta_json import hu_posta_json
            # We only using csekkautomata since there is no XML from another data source
            work = hu_posta_json(session_object(),
                                 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
                                 config.get_directory_cache_url(), 'hu_postacsekkautomata.json')
            _add(work.process() or {})
        else:
            mo = dataproviders_loader.import_module('dataproviders.{0}'.format(module), module)
            work = mo(session_object(), config.get_directory_cache_url())
            insert_type(session_object(), work.types())
            work.process()
            _add(work.export_list())
        logging.debug('Removing session scope for %s module…', module)
        session_object.remove()
        logging.debug('Closing one session for %s module…', module)
        one_session.close()
        logging.info('Finished processing %s module…', module)
        return {'module': module, **stats}
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())
        return {'module': module, 'error': str(e), **stats}
    finally:
        clear_current_provider()


def delete_poi_tables(db: POIBase) -> None:
    """Drop the POI-related tables so they get recreated from scratch (see
    POIBase.__init__'s Base.metadata.create_all()). Called from
    import_poi_data_module() when db.start.drop.poi_tables is True.

    Note: City, Street_type and Country are intentionally not in this list, so
    reference data imported by import_basic_data() (create_db.py) survives a run
    that drops and recreates the POI tables.

    Args:
        db (POIBase): Database wrapper whose engine is used to run the DROP TABLEs.
    """
    bases_to_drop = [
        POI_address,
        POI_address_raw,
        POI_common,
        POI_osm,
        POI_OSM_cache,
        POI_patch
    ]
    
    with db.engine.connect() as conn:
        for base in bases_to_drop:
            table = base.__table__
            logging.info(f'Dropping table {table.name}...')
            conn.execute(text(f'DROP TABLE IF EXISTS {table}'))
    
    logging.info('Dropped all poi_* tables for a clean start.')
