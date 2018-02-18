#!/usr/bin/python
# -*- coding: utf-8 -*-

try:
    import traceback
    import logging, logging.config
    import requests
    import sqlalchemy
    import sqlalchemy.orm
    import pandas as pd
    import re
    import os
    import json
    import hashlib
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs import address, geo
    from osm_poi_matchmaker.dao.data_structure import Base, City, POI_address, POI_common
    from osm_poi_matchmaker.libs.file_output import save_csv_file
    from geoalchemy2 import WKTElement
    import osm_poi_matchmaker.libs.geo
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe, insert_type
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

__program__ = 'create_db'
__version__ = '0.4.0'


POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


def init_log():
    logging.config.fileConfig('log.conf')


class POI_Base:
    """Represents the full database.

    :param db_conection: Either a sqlalchemy database url or a filename to be used with sqlite.

    """

    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.db_filename = None
        if '://' not in db_connection:
            self.db_connection = 'sqlite:///%s' % self.db_connection
        if self.db_connection.startswith('sqlite'):
            self.db_filename = self.db_connection
        self.engine = sqlalchemy.create_engine(self.db_connection, echo=False)
        self.Session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    @property
    def session(self):
        return self.Session()


    def query_all_pd(self, table):
        return pd.read_sql_table(table, self.engine)


def main():
    logging.info('Starting {0} ...'.format(__program__))
    db = POI_Base('{}://{}:{}@{}:{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                            config.get_database_writer_password(), config.get_database_writer_host(),
                                            config.get_database_writer_port()))
    logging.info('Importing cities ...'.format())
    from osm_poi_matchmaker.dataproviders.hu_city_postcode import hu_city_postcode
    work = hu_city_postcode(db.session, '../data/Iranyitoszam-Internet.XLS')
    work.process()

    logging.info('Importing {} stores ...'.format('Tesco'))
    from osm_poi_matchmaker.dataproviders.hu_tesco import hu_tesco
    work = hu_tesco(db.session, 'http://tesco.hu/aruhazak/nyitvatartas/', config.get_directory_cache_url())
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('Aldi'))
    from osm_poi_matchmaker.dataproviders.hu_aldi import hu_aldi
    work = hu_aldi(db.session, 'https://www.aldi.hu/hu/informaciok/informaciok/uezletkereso-es-nyitvatartas/',
                   config.get_directory_cache_url())
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('CBA'))
    from osm_poi_matchmaker.dataproviders.hu_cba import hu_cba
    work = hu_cba(db.session, 'http://www.cba.hu/uzletlista/', config.get_directory_cache_url())
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('Spar'))
    from osm_poi_matchmaker.dataproviders.hu_spar import hu_spar
    work = hu_spar(db.session, 'https://www.spar.hu/bin/aspiag/storefinder/stores?country=HU',
                   config.get_directory_cache_url())
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('Rossmann'))
    from osm_poi_matchmaker.dataproviders.hu_rossmann import hu_rossmann
    work = hu_rossmann(db.session, 'https://www.rossmann.hu/uzletkereso', config.get_directory_cache_url(), verify_link = False)
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('KH Bank'))
    from osm_poi_matchmaker.dataproviders.hu_kh_bank import hu_kh_bank
    work = hu_kh_bank(db.session, os.path.join(config.get_directory_cache_url(), 'hu_kh_bank.json'), 'K&H bank')
    insert_type(db.session, work.types())
    work.process()
    work = hu_kh_bank(db.session, os.path.join(config.get_directory_cache_url(), 'hu_kh_atm.json'), 'K&H')
    work.process()

    logging.info('Importing {} stores ...'.format('BENU'))
    from osm_poi_matchmaker.dataproviders.hu_benu import hu_benu
    work = hu_benu(db.session,
                   'https://benu.hu/wordpress-core/wp-admin/admin-ajax.php?action=asl_load_stores&nonce=1900018ba1&load_all=1&layout=1',
                   config.get_directory_cache_url())
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('Magyar Posta'))
    from osm_poi_matchmaker.dataproviders.hu_posta import hu_posta
    logging.info('Importing {} stores ...'.format('Magyar Posta - posta'))
    work = hu_posta(db.session,
                    'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=posta',
                    config.get_directory_cache_url())
    insert_type(db.session, work.types())
    work.process()
    logging.info('Importing {} stores ...'.format('Magyar Posta - csekkautomata'))
    work = hu_posta(db.session,
                    'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
                    config.get_directory_cache_url(), 'hu_postacsekkautomata.json')
    work.process()
    logging.info('Importing {} stores ...'.format('Magyar Posta - csomagautomata'))
    work = hu_posta(db.session,
                    'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postamachine',
                    config.get_directory_cache_url(), 'hu_postaautomata.json')
    work.process()
    logging.info('Importing {} stores ...'.format('Magyar Posta - postapont'))
    work = hu_posta(db.session,
                    'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postapoint',
                    config.get_directory_cache_url(), 'hu_postapoint.json')
    work.process()

    logging.info('Importing {} stores ...'.format('Penny Market'))
    from osm_poi_matchmaker.dataproviders.hu_penny_market import hu_penny_market
    work = hu_penny_market(db.session, '', config.get_directory_cache_url(), 'penny_market.json')
    insert_type(db.session, work.types())

    logging.info('Importing {} stores ...'.format('Foxpost'))
    from osm_poi_matchmaker.dataproviders.hu_foxpost import hu_foxpost
    work = hu_foxpost(db.session,
                      'http://www.foxpost.hu/wp-content/themes/foxpost/googleapijson.php',
                      config.get_directory_cache_url(),
                      'hu_foxpostautomata.json')
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('Avia'))
    from osm_poi_matchmaker.dataproviders.hu_avia import hu_avia
    work = hu_avia(db.session, 'https://www.avia.hu/kapcsolat/toltoallomasok/', config.get_directory_cache_url(),
                   'hu_avia.html')
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('CIB Bank'))
    from osm_poi_matchmaker.dataproviders.hu_cib_bank import hu_cib_bank
    work = hu_cib_bank(db.session, '', os.path.join(config.get_directory_cache_url(), 'hu_cib_bank.html'), 'CIB bank')
    insert_type(db.session, work.types())
    work = hu_cib_bank(db.session, '', os.path.join(config.get_directory_cache_url(), 'hu_cib_atm.html'), 'CIB')

    logging.info('Exporting CSV files ...')
    targets = ['poi_address', 'poi_common']
    if not os.path.exists(config.get_directory_output()):
        os.makedirs(config.get_directory_output())
    for i in targets:
        data = db.query_all_pd(i)
        save_csv_file(config.get_directory_output(), '{}.csv'.format(i), data, i)


if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    init_log()
    main()
