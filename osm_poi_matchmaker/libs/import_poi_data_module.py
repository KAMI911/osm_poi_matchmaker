# -*- coding: utf-8 -*-

__author__ = 'kami911'

try:
    import logging
    import sys
    import os
    import traceback
    from sqlalchemy.orm import scoped_session, sessionmaker
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from osm_poi_matchmaker.utils import config, dataproviders_loader
    from osm_poi_matchmaker.dao.data_handlers import insert_type, get_or_create
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def import_poi_data_module(module: str):
    """Process all data provider modules enabled in app.conf and write to the database

    Args:
        module (str): Name of module to run
    """
    try:
        db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                                  config.get_database_writer_password(),
                                                  config.get_database_writer_host(),
                                                  config.get_database_writer_port(),
                                                  config.get_database_poi_database()))
        pgsql_pool = db.pool
        session_factory = sessionmaker(pgsql_pool)
        Session = scoped_session(session_factory)
        session = Session()
        module = module.strip()
        logging.info('Processing %s module ...', module)
        if module == 'hu_kh_bank':
            from osm_poi_matchmaker.dataproviders.hu_kh_bank import hu_kh_bank
            work = hu_kh_bank(session, config.get_directory_cache_url(), True,
                              os.path.join(config.get_directory_cache_url(), 'hu_kh_bank.json'), 'K&H Bank')
            insert_type(session, work.types())
            work.process()
            work = hu_kh_bank(session, config.get_directory_cache_url(), True,
                              os.path.join(config.get_directory_cache_url(), 'hu_kh_atm.json'), 'K&H Bank ATM')
            work.process()
        elif module == 'hu_cib_bank':
            from osm_poi_matchmaker.dataproviders.hu_cib_bank import hu_cib_bank
            work = hu_cib_bank(session, config.get_directory_cache_url(), True,
                               os.path.join(config.get_directory_cache_url(), 'hu_cib_bank.json'), 'CIB Bank')
            insert_type(session, work.types())
            work.process()
            work = hu_cib_bank(session, config.get_directory_cache_url(), True,
                               os.path.join(config.get_directory_cache_url(), 'hu_cib_atm.json'), 'CIB Bank ATM')
            work.process()
        elif module == 'hu_posta_json':
            # Old code that uses JSON files
            from osm_poi_matchmaker.dataproviders.hu_posta_json import hu_posta_json
            # We only using csekkautomata since there is no XML from another data source
            work = hu_posta_json(session,
                                 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
                                 config.get_directory_cache_url(), 'hu_postacsekkautomata.json')
            work.process()
        else:
            mo = dataproviders_loader.import_module('dataproviders.{0}'.format(module), module)
            work = mo(session, config.get_directory_cache_url())
            insert_type(session, work.types())
            work.process()
            work.export_list()
            logging.info('Finished processing %s module ...', module)
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.error(traceback.print_exc())
