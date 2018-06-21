#!/usr/bin/python
# -*- coding: utf-8 -*-

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
    from osm_poi_matchmaker.utils import config, timing, dataproviders_loader
    from osm_poi_matchmaker.dao.data_structure import Base
    from osm_poi_matchmaker.libs.file_output import save_csv_file, generate_osm_xml
    from osm_poi_matchmaker.dao.data_handlers import insert_type
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

__program__ = 'create_db'
__version__ = '0.4.7'


POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


def init_log():
    logging.config.fileConfig('log.conf')


def import_basic_data(session):
    logging.info('Importing cities ...'.format())
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_city_postcode_from_xml
    work = hu_city_postcode_from_xml(session, 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/ZipCodes.xml', config.get_directory_cache_url())
    work.process()

    logging.info('Importing street types ...'.format())
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_street_types_from_xml
    work = hu_street_types_from_xml(session, 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/StreetTypes.xml', config.get_directory_cache_url())
    work.process()


def import_poi_data(session):
    for module in config.get_dataproviders_modules_enable():
        module = module.strip()
        logging.info('Processing {} module ...'.format(module))
        mo = dataproviders_loader.import_module('osm_poi_matchmaker.dataproviders.{0}'.format(module), module)
        work = mo(session, config.get_directory_cache_url(), config.get_geo_prefer_osm_postcode())
        insert_type(session, work.types())
        work.process()

    logging.info('Importing {} stores ...'.format('KH Bank'))
    from osm_poi_matchmaker.dataproviders.hu_kh_bank import hu_kh_bank
    work = hu_kh_bank(session, config.get_directory_cache_url(), config.get_geo_prefer_osm_postcode(), os.path.join(config.get_directory_cache_url(), 'hu_kh_bank.json'), 'K&H bank')
    insert_type(session, work.types())
    work.process()
    work = hu_kh_bank(session, config.get_directory_cache_url(), config.get_geo_prefer_osm_postcode(), os.path.join(config.get_directory_cache_url(), 'hu_kh_atm.json'), 'K&H')
    work.process()

    # Old code that uses JSON files
    from osm_poi_matchmaker.dataproviders.hu_posta_json import hu_posta_json
    # We only using csekkautomata since there is no XML from another data source
    logging.info('Importing {} stores ...'.format('Magyar Posta - csekkautomata'))
    work = hu_posta_json(session,
                    'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
                    config.get_directory_cache_url(), 'hu_postacsekkautomata.json')
    work.process()

    logging.info('Importing {} stores ...'.format('CIB Bank'))
    from osm_poi_matchmaker.dataproviders.hu_cib_bank import hu_cib_bank
    work = hu_cib_bank(session, '', os.path.join(config.get_directory_cache_url(), 'hu_cib_bank.html'), config.get_geo_prefer_osm_postcode(), 'CIB bank')
    insert_type(session, work.types())
    work = hu_cib_bank(session, '', os.path.join(config.get_directory_cache_url(), 'hu_cib_atm.html'), config.get_geo_prefer_osm_postcode(), 'CIB')


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
        '''
        Load all POI data from SQL
        :param table: Name of table where POI data is stored
        :return: Full table read from SQL database table
        '''
        return pd.read_sql_table(table, self.engine)

    def query_all_gpd(self, table):
        '''
        Load all POI data from SQL that contains gometry
        :param table: Name of table where POI data is stored
        :return: Full table with poi_lat and poi_long fileds read from SQL database table
        '''
        query = sqlalchemy.text('select * from {} where poi_geom is not NULL'.format(table))
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='poi_geom')
        data['poi_lat'] = data['poi_geom'].x
        data['poi_lon'] = data['poi_geom'].y
        return data

    def query_osm_shop_poi_gpd(self, lon, lat, ptype='shop'):
        '''
        Search for POI in OpenStreetMap database based on POI type and geom within preconfigured distance
        :param lon:
        :param lat:
        :param ptype:
        :return:
        '''
        if ptype == 'shop':
            query_type = "shop='convenience' OR shop='supermarket'"
        elif ptype == 'fuel':
            query_type = "amenity='fuel'"
        elif ptype == 'bank':
            query_type = "amenity='bank'"
        elif ptype == 'atm':
            query_type = "amenity='atm'"
        elif ptype == 'post_office':
            query_type = "amenity='post_office'"
        elif ptype == 'vending_machine':
            query_type = "amenity='vending_machine'"
        elif ptype == 'pharmacy':
            query_type = "amenity='vending_machine'"
        elif ptype == 'chemist':
            query_type = "shop='chemist'"
        elif ptype == 'bicycle_rental':
            query_type = "amenity='bicycle_rental'"
        query = sqlalchemy.text('''
            SELECT name,osm_id, false::boolean AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", amenity, ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
            WHERE ({type}) AND osm_id > 0
                AND ST_DWithin(ST_Centroid(way),ST_Transform(point.geom,3857), :distance)
            ORDER BY distance ASC;'''.format(type=query_type))
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat, 'distance': config.get_geo_default_poi_distance()})
        query = sqlalchemy.text('''
            SELECT name,osm_id, true::boolean AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", amenity, ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way
            FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
            WHERE ({type}) AND osm_id > 0
                AND ST_DWithin(way,ST_Transform(point.geom,3857), :distance)
            ORDER BY distance ASC;'''.format(type=query_type))
        data2 = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat, 'type': query_type, 'distance': config.get_geo_default_poi_distance()})
        data = data.append(data2)
        return data.sort_values(by=['distance'])

    def query_ways_nodes(self, way_id):
        if way_id > 0:
            query = sqlalchemy.text('select nodes from planet_osm_ways where id = :way_id limit 1')
            data = pd.read_sql(query, self.engine, params={'way_id': int(way_id)})
            return data.values.tolist()[0][0]
        else:
            return None

    def query_osm_shop_poi_gpd_with_metadata(self, lon, lat, ptype='shop'):
        '''
        Search for POI in OpenStreetMap database based on POI type and geom within preconfigured distance
        :param lon:
        :param lat:
        :param ptype:
        :return:
        '''
        if ptype == 'shop':
            query_type = "shop='convenience' OR shop='supermarket'"
        elif ptype == 'fuel':
            query_type = "amenity='fuel'"
        elif ptype == 'bank':
            query_type = "amenity='bank'"
        elif ptype == 'atm':
            query_type = "amenity='atm'"
        elif ptype == 'post_office':
            query_type = "amenity='post_office'"
        elif ptype == 'vending_machine':
            query_type = "amenity='vending_machine'"
        elif ptype == 'pharmacy':
            query_type = "amenity='vending_machine'"
        elif ptype == 'chemist':
            query_type = "shop='chemist'"
        elif ptype == 'bicycle_rental':
            query_type = "amenity='bicycle_rental'"
        query = sqlalchemy.text('''
            SELECT name, osm_id, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, false::boolean AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", amenity, ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
            WHERE ({type}) AND osm_id > 0
                AND ST_DWithin(ST_Centroid(way),ST_Transform(point.geom,3857), :distance)
            ORDER BY distance ASC;'''.format(type=query_type))
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat, 'distance': config.get_geo_default_poi_distance()})
        query = sqlalchemy.text('''
            SELECT name, osm_id, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, true::boolean AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", amenity, ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way
            FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
            WHERE ({type}) AND osm_id > 0
                AND ST_DWithin(way,ST_Transform(point.geom,3857), :distance)
            ORDER BY distance ASC;'''.format(type=query_type))
        data2 = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat, 'type': query_type, 'distance': config.get_geo_default_poi_distance()})
        if data2.empty == False:
            if data.empty == False:
                data = data.append(data2)
                return data.sort_values(by=['distance'])
            else:
                return data2
        else:
            if data.empty == False:
                return data


def main():
    logging.info('Starting {0} ...'.format(__program__))
    db = POIBase('{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                              config.get_database_writer_password(), config.get_database_writer_host(),
                                              config.get_database_writer_port(), config.get_database_poi_database()))

    import_basic_data(db.session)
    import_poi_data(db.session)

    logging.info('Exporting CSV files ...')
    if not os.path.exists(config.get_directory_output()):
        os.makedirs(config.get_directory_output())
    # Build Dataframe from our POI database
    addr_data = db.query_all_gpd('poi_address')
    addr_data[['poi_addr_city', 'poi_postcode']] = addr_data[['poi_addr_city', 'poi_postcode']].fillna('0').astype(int)
    comm_data = db.query_all_pd('poi_common')
    # And merge and them into one Dataframe and save it to a CSV file
    save_csv_file(config.get_directory_output(), 'poi_common.csv', comm_data, 'poi_common')
    data = pd.merge(addr_data, comm_data, left_on='poi_common_id', right_on='pc_id', how='inner')
    save_csv_file(config.get_directory_output(), 'poi_address.csv', data, 'poi_address')
    # Generating CSV files group by poi_code
    poi_codes = data.poi_code.unique()
    # Adding additional empty fields
    data['osm_id'] = None
    data['node'] = None
    data['osm_version'] = None
    data['osm_changeset'] = None
    data['osm_timestamp'] = None
    for c in poi_codes:
        save_csv_file(config.get_directory_output(), 'poi_address_{}.csv'.format(c), data[data.poi_code == c], 'poi_address')
        with open(os.path.join(config.get_directory_output(), 'poi_address_{}.osm'.format(c)), 'wb') as oxf:
            oxf.write(generate_osm_xml(data[data.poi_code == c]))
    with open(os.path.join(config.get_directory_output(), 'poi_address.osm'), 'wb') as oxf:
        oxf.write(generate_osm_xml(data))

    logging.info('Merging with OSM datasets ...')
    counter = 0
    data['osm_nodes'] = None
    from datetime import datetime
    for i, row in data.iterrows():
        common_row = comm_data.loc[comm_data['pc_id'] == row['poi_common_id']]
        osm_query = (db.query_osm_shop_poi_gpd_with_metadata(row['poi_lon'], row['poi_lat'], common_row['poi_type'].item()))
        if osm_query is not None:
            # Collect additional OSM metadata. Note: this needs style change during osm2pgsql
            osm_id = osm_query['osm_id'].values[0]
            data.loc[[i], 'osm_id'] = osm_id
            data.loc[[i], 'node'] = osm_query['node'].values[0]
            data.loc[[i], 'osm_version'] = osm_query['osm_version'].values[0]
            data.loc[[i], 'osm_changeset'] = osm_query['osm_changeset'].values[0]
            osm_timestamp = pd.to_datetime(str((osm_query['osm_timestamp'].values[0])))
            data.loc[[i], 'osm_timestamp'] = '{:{dfmt}T{tfmt}Z}'.format(osm_timestamp, dfmt='%Y-%m-%d', tfmt='%H:%M%S')
            # For OSM way also query node points
            if osm_query['node'].values[0] ==  False:
                logging.info('This is an OSM way looking for id {} nodes.'.format(osm_id))
                # Add list of nodes to the dataframe
                nodes = db.query_ways_nodes(osm_id)
                data.at[i, 'osm_nodes'] = nodes

            counter +=1
    for c in poi_codes:
        save_csv_file(config.get_directory_output(), 'poi_address_merge_{}.csv'.format(c), data[data.poi_code == c], 'poi_address')
        with open(os.path.join(config.get_directory_output(), 'poi_address_merge_{}.osm'.format(c)), 'wb') as oxf:
            oxf.write(generate_osm_xml(data[data.poi_code == c]))
    with open(os.path.join(config.get_directory_output(), 'poi_address_merge.osm'), 'wb') as oxf:
        oxf.write(generate_osm_xml(data))
    logging.info('{} objects found in OSM dataset.'.format(counter))

if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    init_log()
    timer = timing.Timing()
    main()
    logging.info('Total duration of process: {}. Finished, exiting and go home ...'.format(timer.end()))
