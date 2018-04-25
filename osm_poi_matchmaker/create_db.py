#!/usr/bin/python
# -*- coding: utf-8 -*-

try:
    import os
    import traceback
    import logging
    import logging.config
    import sqlalchemy
    import sqlalchemy.orm
    import pandas as pd
    import geopandas as gpd
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.dao.data_structure import Base
    from osm_poi_matchmaker.libs.file_output import save_csv_file, generate_osm_xml
    from osm_poi_matchmaker.dao.data_handlers import insert_type
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

__program__ = 'create_db'
__version__ = '0.4.5'


POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


def init_log():
    logging.config.fileConfig('log.conf')


class POIBase:
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

    def query_all_gpd(self, table):
        query = sqlalchemy.text('select * from {} where poi_geom is not NULL'.format(table))
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='poi_geom')
        data['poi_lat'] = data['poi_geom'].x
        data['poi_lon'] = data['poi_geom'].y
        return data

def main():
    logging.info('Starting {0} ...'.format(__program__))
    db = POIBase('{}://{}:{}@{}:{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                           config.get_database_writer_password(), config.get_database_writer_host(),
                                           config.get_database_writer_port()))

    logging.info('Importing cities ...'.format())
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_city_postcode_from_xml
    work = hu_city_postcode_from_xml(db.session, 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/ZipCodes.xml', config.get_directory_cache_url())
    work.process()

    logging.info('Importing street types ...'.format())
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_street_types_from_xml
    work = hu_street_types_from_xml(db.session, 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/StreetTypes.xml', config.get_directory_cache_url())
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
    work = hu_rossmann(db.session, 'https://www.rossmann.hu/uzletkereso', config.get_directory_cache_url(),
                       verify_link=False)
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

    logging.info('Importing {} stores ...'.format('Kulcs patika'))
    from osm_poi_matchmaker.dataproviders.hu_kulcs_patika import hu_kulcs_patika
    work = hu_kulcs_patika(db.session, 'http://kulcspatika.hu/inc/getPagerContent.php?tipus=patika&kepnelkul=true&latitude=47.498&longitude=19.0399', os.path.join(config.get_directory_cache_url()))
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

    logging.info('Importing {} stores ...'.format('MOL'))
    from osm_poi_matchmaker.dataproviders.hu_mol import hu_mol
    work = hu_mol(db.session, 'http://toltoallomaskereso.mol.hu/hu/portlet/routing/along_latlng.json', config.get_directory_cache_url(),
                  'hu_mol.json')
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('OMV'))
    from osm_poi_matchmaker.dataproviders.hu_omv import hu_omv
    work = hu_omv(db.session, 'http://webgispu.wigeogis.com/kunden/omvpetrom/backend/getFsForCountry.php', config.get_directory_cache_url(),
                  'hu_omv.json')
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('Shell'))
    from osm_poi_matchmaker.dataproviders.hu_shell import hu_shell
    work = hu_shell(db.session, 'https://locator.shell.hu/deliver_country_csv.csv?footprint=HU&site=cf&launch_country=HU&networks=ALL', config.get_directory_cache_url(),
                    'hu_shell.csv')
    insert_type(db.session, work.types())
    work.process()

    logging.info('Importing {} stores ...'.format('CIB Bank'))
    from osm_poi_matchmaker.dataproviders.hu_cib_bank import hu_cib_bank
    work = hu_cib_bank(db.session, '', os.path.join(config.get_directory_cache_url(), 'hu_cib_bank.html'), 'CIB bank')
    insert_type(db.session, work.types())
    work = hu_cib_bank(db.session, '', os.path.join(config.get_directory_cache_url(), 'hu_cib_atm.html'), 'CIB')

    logging.info('Importing {} stores ...'.format('Tom Market'))
    from osm_poi_matchmaker.dataproviders.hu_tommarket import hu_tom_market
    work = hu_tom_market(db.session, 'http://tommarket.hu/shops', config.get_directory_cache_url(),
                         'hu_tom_market.html')
    insert_type(db.session, work.types())
    '''
    work.process()
    '''
    logging.info('Exporting CSV files ...')
    if not os.path.exists(config.get_directory_output()):
        os.makedirs(config.get_directory_output())
    addr_data = db.query_all_gpd('poi_address')
    addr_data[['poi_addr_city', 'poi_postcode']] = addr_data[['poi_addr_city', 'poi_postcode']].fillna('0').astype(int)
    comm_data = db.query_all_pd('poi_common')
    save_csv_file(config.get_directory_output(), 'poi_common.csv', comm_data, 'poi_common')
    data = pd.merge(addr_data, comm_data, left_on='poi_common_id', right_on='pc_id', how='inner')
    save_csv_file(config.get_directory_output(), 'poi_address.csv', data, 'poi_address')
    with open(os.path.join(config.get_directory_output(), 'poi_address.osm'), 'wb') as oxf:
        oxf.write(generate_osm_xml(data))


if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    init_log()
    main()
