# -*- coding: utf-8 -*-
try:
    import logging
    import sys
    import geopandas as gpd
    import pandas as pd
    import sqlalchemy
    #from sqlalchemy.orm import scoped_session, sessionmaker
    import time
    import math
    from math import isnan
    from osm_poi_matchmaker.utils import config, poitypes
    from osm_poi_matchmaker.dao.data_structure import Base
    import psycopg2
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class POIBase:
    """Represents the full database.

    :param db_conection: Either a sqlalchemy database url or a filename to be used with sqlite.

    """

    def __init__(self, db_connection, retry_counter=100, retry_sleep=30):
        reco = 0  # Actual retry counter
        self.db_retry_counter = retry_counter
        self.db_retry_sleep = retry_sleep
        self.db_connection = db_connection
        self.db_filename = None
        if '://' not in db_connection:
            self.db_connection = 'sqlite:///{}'.format(self.db_connection)
        if self.db_connection.startswith('sqlite'):
            self.db_filename = self.db_connection
        try:
            self.engine = sqlalchemy.create_engine(self.db_connection, client_encoding='utf8',
                                                   echo=config.get_database_enable_query_log(), pool_size=40,
                                                   max_overflow=20, pool_pre_ping=True, pool_use_lifo=True)
        except psycopg2.OperationalError as e:
            logging.error('Database error: %s', e)
            if self.retry_counter >= reco:
                logging.error('Cannot connect to database with %s connection string', self.db_connection)
            else:
                logging.error('Cannot connect to the database. It will retry within %s seconds. [%s/%s]',
                              self.db_retry_sleep, reco, self.db_retry_counter)
                time.sleep(self.db_retry_sleep)
                self.engine = sqlalchemy.create_engine(self.db_connection, client_encoding='utf8',
                                                       echo=config.get_database_enable_query_log(), pool_size=40,
                                                       max_overflow=20, pool_pre_ping=True, pool_use_lifo=True)
                self.db_retry_counter += 1
        Base.metadata.create_all(self.engine)
        self.one_connection = self.engine.connect()
        self.session_maker = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.session_object = sqlalchemy.orm.scoped_session(self.session_maker)
        self.one_session = self.session_object()

    @property
    def pool(self):
        return self.engine

    @property
    def session(self):
        return self.session_object()

    def connection(self):
        return self.engine.connect()

    def __del__(self):
        if self.one_session:
            self.one_session.close()
            self.engine.dispose()

    def query_all_pd(self, table):

        '''
        Load all POI data from SQL
        :param table: Name of table where POI data is stored
        :return: Full table read from SQL database table
        '''
        return pd.read_sql_table(table, self.connection())

    def query_all_gpd(self, table):
        '''
        Load all POI data from SQL that contains gometry
        :param table: Name of table where POI data is stored
        :return: Full table with poi_lat and poi_long fileds read from SQL database table
        '''
        query = sqlalchemy.text('select * from {} where poi_geom is not NULL'.format(table))
        data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='poi_geom')
        data['poi_lat'] = data['poi_geom'].x
        data['poi_lon'] = data['poi_geom'].y
        return data

    def query_all_gpd_in_order(self, table):
        '''
        Load all POI data from SQL that contains gometry and ordered by poi_common_id and postcode
        :param table: Name of table where POI data is stored
        :return: Full table with poi_lat and poi_long fileds read from SQL database table
        '''
        query = sqlalchemy.text('''SELECT * FROM {}
                                     WHERE poi_geom is not NULL
                                     ORDER BY poi_common_id ASC, poi_postcode ASC, poi_addr_street ASC,
                                       poi_addr_housenumber ASC'''.format(table))
        data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='poi_geom')
        data['poi_lat'] = data['poi_geom'].x
        data['poi_lon'] = data['poi_geom'].y
        return data

    def count_all_gpd(self, table):
        '''
        Load all POI data from SQL that contains geometry
        :param table: Name of table where POI data is stored
        :return: Full table with poi_lat and poi_long fields read from SQL database table
        '''
        query = sqlalchemy.text('select count(*) from {} where poi_geom is not NULL'.format(table))
        data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='poi_geom')
        return data

    def query_from_cache(self, node_id, object_type):
        if node_id > 0:
            query = sqlalchemy.text(
                'select * from poi_osm_cache where osm_id = :node_id and osm_object_type = :object_type limit 1')
            data = pd.read_sql(query, self.connection(), params={'node_id': int(node_id), 'object_type': object_type.name})
            if not data.values.tolist():
                return None
            else:
                return data.to_dict('records')[0]
        else:
            return None

    def query_ways_nodes(self, way_id):
        if way_id > 0:
            query = sqlalchemy.text('select nodes from planet_osm_ways where id = :way_id limit 1')
            data = pd.read_sql(query, self.connection(), params={'way_id': int(way_id)})
            return data.values.tolist()[0][0]
        else:
            return None

    def query_relation_nodes(self, relation_id):
        query = sqlalchemy.text('select members from planet_osm_rels where id = :relation_id limit 1')
        data = pd.read_sql(query, self.connection(), params={'relation_id': int(abs(relation_id))})
        return data.values.tolist()[0][0]

    def query_osm_shop_poi_gpd(self, lon: float, lat: float, ptype: str = 'shop', name: str = None,
                               avoid_name: str = None, unique_name: str = None,
                               additional_ref_name: str = None, unique_ref: str = None,
                               street_name: str = None, housenumber: str = None, conscriptionnumber: str = None,
                               city: str = None, distance_perfect: int = None, distance_safe: int = None,
                               distance_unsafe: int = None, with_metadata: bool = True):
        '''
        Search for POI in OpenStreetMap database based on POI type and geom within preconfigured distance
        :param lon:
        :param lat:
        :param ptype:
        :param name:
        :param avoid_name:
        :param unique_name: Individual name of selected POI
        :param additional_ref_name: name of additional reference type
        :param unique_ref:  Individual reference number of additional reference type
        :param street_name:
        :param housenumber:
        :param conscriptionnumber:
        :param city:
        :param distance_perfect:
        :param distance_safe:
        :param distance_unsafe:
        :param with_metadata:
        :return:
        '''
        buffer = 10
        query_arr = []
        query_params = {}
        query_type, distance = poitypes.getPOITypes(ptype)
        # If we have PO common defined unsafe search radius distance, then use it (or use defaults specified above)
        if distance_unsafe is None or distance_unsafe == '' or math.isnan(distance_unsafe):
            distance_unsafe = config.get_geo_default_poi_unsafe_distance()
        query_params.update({'distance_unsafe': distance_unsafe})
        if distance_safe is None or distance_safe == '' or math.isnan(distance_safe):
            distance_safe = config.get_geo_default_poi_distance()
        query_params.update({'distance_safe': distance_safe})
        if lon is not None and lon != '':
            query_params.update({'lon': lon})
        if lat is not None and lat != '':
            query_params.update({'lat': lat})
        query_params.update({'buffer': buffer})
        # Do not match with other specified names, brand names, network names
        if name is not None and name != '':
            query_name = 'AND (LOWER(TEXT(name)) ~* LOWER(TEXT(:name)) OR LOWER(TEXT(brand)) ~* LOWER(TEXT(:name)) OR LOWER(TEXT(network)) ~* LOWER(TEXT(:name))) '
            query_params.update({'name': '.*{}.*'.format(name)})
            # If we have PO common defined safe search radius distance, then use it (or use defaults specified above)
            if distance_perfect is None or distance_perfect != '' or math.isnan(distance_perfect):
                distance_perfect = config.get_geo_default_poi_perfect_distance()
            query_params.update({'distance_perfect': distance_perfect})
        else:
            query_name = ''
        # Do not match with other specified names, brand names, network names
        if avoid_name is not None and avoid_name != '':
            query_avoid_name = 'AND (LOWER(TEXT(name)) !~* LOWER(TEXT(:avoid_name)) AND LOWER(TEXT(brand)) !~* LOWER(TEXT(:avoid_name)) AND LOWER(TEXT(network)) !~* LOWER(TEXT(:avoid_name))) '
            query_params.update({'avoid_name': '.*{}.*'.format(avoid_name)})
        else:
            query_avoid_name = ''
        if unique_name is not None and unique_name != '':
            query_unique_name = ' AND LOWER(TEXT(name)) = LOWER(TEXT(:unique_name))'
            query_params.update({'unique_name': unique_name})
        else:
            query_unique_name = ''
        if additional_ref_name is not None and additional_ref_name != '' and unique_ref is not None and unique_ref != '':
            query_unique_ref = ' AND LOWER(TEXT(ref:{})) = LOWER(TEXT(:unique_ref))'.format(additional_ref_name)
            query_params.update({'unique_ref': unique_ref})
        else:
            query_unique_ref = ''
        if with_metadata is True:
            metadata_fields = ' osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, '
        else:
            metadata_fields = ''
        if street_name is not None and street_name != '':
            street_query = ' AND LOWER(TEXT("addr:street")) = LOWER(TEXT(:street_name))'
            query_params.update({'street_name': street_name})
        else:
            street_query = ''
        if housenumber is not None and housenumber != '':
            housenumber_query = ' AND LOWER(TEXT("addr:housenumber")) = LOWER(TEXT(:housenumber))'
            query_params.update({'housenumber': housenumber})
        else:
            housenumber_query = ''
        if conscriptionnumber is not None and conscriptionnumber != '':
            conscriptionnumber_query = ' AND LOWER(TEXT("addr:conscriptionnumber")) = LOWER(TEXT(:conscriptionnumber))'
            query_params.update({'conscriptionnumber': conscriptionnumber})
        else:
            conscriptionnumber_query = ''
        if city is not None and city != '':
            city_query = ' AND LOWER(TEXT("addr:city")) = LOWER(TEXT(:city))'
            query_params.update({'city': city})
        else:
            city_query = ''
        logging.debug('%s %s: %s, %s (NOT %s), %s %s %s (%s) [%s, %s, %s]', lon, lat, ptype, name, avoid_name, city,
                      street_name, housenumber, conscriptionnumber, distance_perfect, distance_safe, distance_unsafe)
        if additional_ref_name is not None and additional_ref_name != '' and unique_ref is not None and unique_ref != '':
            query_text = '''
            --- WITH ADDITIONAL REF
            --- The way selector with additional ref
            SELECT name, osm_id, {metadata_fields} 930 AS priority, 'way' AS node, highway, "ref:{additional_ref_name}",
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon
            WHERE ({query_type}) AND osm_id > 0 {query_unique_ref}
            UNION ALL
            --- The node selector with with additional ref
            SELECT name, osm_id, {metadata_fields} 930 AS priority, 'node' AS node, highway, "ref:{additional_ref_name}",
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(planet_osm_point.way) as lon,
                   ST_Y(planet_osm_point.way) as lat
            FROM planet_osm_point
            WHERE ({query_type}) AND osm_id > 0 {query_unique_ref}
            UNION ALL
            --- The relation selector with additional ref
            SELECT name, osm_id, {metadata_fields} 930 AS priority, 'relation' AS node, highway, "ref:{additional_ref_name}",
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon
            WHERE ({query_type}) AND osm_id < 0 {query_unique_ref}
            '''
            query = sqlalchemy.text(query_text.format(query_type=query_type,
                                                      metadata_fields=metadata_fields,
                                                      additional_ref_name=additional_ref_name,
                                                      query_unique_ref=query_unique_ref))
            logging.debug(str(query))
            #  Make EXPLAIN ANALYZE of long queries configurable with issue #99
            if config.get_database_enable_analyze() is True:
                try:
                    perf_query = sqlalchemy.text('EXPLAIN ANALYZE ' + query_text.format(query_type=query_type,
                                                      metadata_fields=metadata_fields,
                                                      additional_ref_name=additional_ref_name,
                                                      query_unique_ref=query_unique_ref))
                    perf = self.session.execute(perf_query, query_params)
                except Exception as err:
                    logging.warning('Exception occurred')
                    logging.exception(err)
                #logging.critical(perf.mappings().all())
                perf_rows = [str(row.values()) for row in perf.mappings().all()]
                logging.debug('\n'.join(perf_rows))
            try:
                data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='way', params=query_params)
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            if not data.empty:
                logging.info('Found item with simple additional ref search.')
                logging.debug(data.to_string())
                logging.debug('Return with additional ref search data: {}'.format(data.iloc[[0]]))
                return data.iloc[[0]]
            else:
                logging.info('Item not found with additional ref search.')
        if unique_name is not None and unique_name != '':
            query_text = '''
            --- WITH UNIQUE NAME
            --- The way selector with unique name
            SELECT name, osm_id, {metadata_fields} 930 AS priority, 'way' AS node, highway,
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon
            WHERE ({query_type}) AND osm_id > 0 {query_unique_name}
            UNION ALL
            --- The node selector with unique name
            SELECT name, osm_id, {metadata_fields} 930 AS priority, 'node' AS node, highway,
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(planet_osm_point.way) as lon,
                   ST_Y(planet_osm_point.way) as lat
            FROM planet_osm_point
            WHERE ({query_type}) AND osm_id > 0 {query_unique_name}
            UNION ALL
            --- The relation selector with unique name
            SELECT name, osm_id, {metadata_fields} 930 AS priority, 'relation' AS node, highway,
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon
            WHERE ({query_type}) AND osm_id < 0 {query_unique_name}
            '''
            try:
                query = sqlalchemy.text(query_text.format(query_type=query_type,
                                                      metadata_fields=metadata_fields,
                                                      query_unique_name=query_unique_name))
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            logging.debug(str(query))
            #  Make EXPLAIN ANALYZE of long queries configurable with issue #99
            if config.get_database_enable_analyze() is True:
                try:
                    perf_query = sqlalchemy.text('EXPLAIN ANALYZE ' + query_text.format(query_type=query_type,
                                                      metadata_fields=metadata_fields,
                                                      query_unique_name=query_unique_name))
                    perf = self.session.execute(perf_query, query_params)
                except Exception as err:
                    logging.warning('Exception occurred')
                    logging.exception(err)
                #logging.critical(perf.mappings().all())
                perf_rows = [str(row.values()) for row in perf.mappings().all()]
                logging.debug('\n'.join(perf_rows))
            try:
                data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='way', params=query_params)
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            if not data.empty:
                if len(data) > 1:
                    logging.info('Multiple items found with simple unique name search. Skipping this method ...')
                else:
                    logging.info('Found item with simple unique name search.')
                    logging.debug(data.to_string())
                    logging.debug('Return with additional unique name data: {}'.format(data.iloc[[0]]))
                    return data.iloc[[0]]
            else:
                logging.info('Item not found with unique name search.')
        if query_name is not None and query_name != '' and city_query is not None and city_query != '' and \
                conscriptionnumber_query is not None and conscriptionnumber_query != '':
            query_text = '''
            --- WITH NAME, WITH CONSCRIPTINNUMBER, WITH CITY
            --- The way selector with conscriptionnumber and city
            SELECT name, osm_id, {metadata_fields} 965 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                   "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon
            WHERE ({query_type}) AND osm_id > 0 {query_name} {conscriptionnumber_query} {city_query}
            UNION ALL
            --- The node selector with conscriptionnumber and city
            SELECT name, osm_id, {metadata_fields} 965 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                   "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(planet_osm_point.way) as lon,
                   ST_Y(planet_osm_point.way) as lat
            FROM planet_osm_point
            WHERE ({query_type}) AND osm_id > 0 {query_name} {conscriptionnumber_query} {city_query}
            UNION ALL
            --- The relation selector with conscriptionnumber and city
            SELECT name, osm_id, {metadata_fields} 965 AS priority, 'relation' AS node, shop, amenity,
                   "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                   "addr:conscriptionnumber", '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon
            WHERE ({query_type}) AND osm_id < 0 {query_name} {conscriptionnumber_query} {city_query}
            '''
            try:
                query = sqlalchemy.text(query_text.format(query_type=query_type, query_name=query_name,
                                                      metadata_fields=metadata_fields,
                                                      conscriptionnumber_query=conscriptionnumber_query,
                                                      city_query=city_query))
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            logging.debug(str(query))
            #  Make EXPLAIN ANALYZE of long queries configurable with issue #99
            if config.get_database_enable_analyze() is True:
                try:
                    perf_query = sqlalchemy.text('EXPLAIN ANALYZE ' + query_text.format(query_type=query_type,
                                                                                    query_name=query_name,
                                                                                    metadata_fields=metadata_fields,
                                                                                    conscriptionnumber_query=conscriptionnumber_query,
                                                                                    city_query=city_query))
                    perf = self.session.execute(perf_query, query_params)
                except Exception as err:
                    logging.warning('Exception occurred')
                    logging.exception(err)
                #logging.critical(perf.mappings().all())
                perf_rows = [str(row.values()) for row in perf.mappings().all()]
                logging.debug('\n'.join(perf_rows))
            try:
                data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='way', params=query_params)
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            if not data.empty:
                logging.info('Found item with simple conscription number search.')
                logging.debug(data.to_string())
                logging.debug('Return with simple conscription number search data: {}'.format(data.iloc[[0]]))
                return data.iloc[[0]]
            else:
                logging.info('Item not found with simple conscription number search.')
        if query_name is not None and query_name != '' and city_query is not None and city_query != '' and \
                street_query is not None and street_query != '' and \
                housenumber_query is not None and housenumber_query != '':
            query_text = '''
            --- WITH NAME, WITH CITY, WITH STREETNAME, WITH HOUSENUMBER
            --- The way selector with city, street name and housenumber
            SELECT name, osm_id, {metadata_fields} 940 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                   "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon
            WHERE ({query_type}) AND osm_id > 0 {query_name} {city_query} {street_query} {housenumber_query}
            UNION ALL
            --- The node selector with street name and housenumber
            SELECT name, osm_id, {metadata_fields} 940 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                   "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                   '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(planet_osm_point.way) as lon,
                   ST_Y(planet_osm_point.way) as lat
            FROM planet_osm_point
            WHERE ({query_type}) AND osm_id > 0 {query_name} {city_query} {street_query} {housenumber_query}
            UNION ALL
            --- The relation selector with street name and housenumber
            SELECT name, osm_id, {metadata_fields} 940 AS priority, 'relation' AS node, shop, amenity,
                   "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                   "addr:conscriptionnumber", '0' as distance, way, ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon
            WHERE ({query_type}) AND osm_id < 0 {query_name} {city_query} {street_query} {housenumber_query}
            '''
            try:
                query = sqlalchemy.text(query_text.format(query_type=query_type, query_name=query_name,
                                                      metadata_fields=metadata_fields,
                                                      street_query=street_query,
                                                      city_query=city_query,
                                                      housenumber_query=housenumber_query))
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            logging.debug(str(query))
            #  Make EXPLAIN ANALYZE of long queries configurable with issue #99
            if config.get_database_enable_analyze() is True:
                try:
                    perf_query = sqlalchemy.text('EXPLAIN ANALYZE ' + query_text.format(query_type=query_type,
                                                                                    query_name=query_name,
                                                                                    query_avoid_name=query_avoid_name,
                                                                                    metadata_fields=metadata_fields,
                                                                                    street_query=street_query,
                                                                                    city_query=city_query,
                                                                                    housenumber_query=housenumber_query))
                    perf = self.session.execute(perf_query, query_params)
                except Exception as err:
                    logging.warning('Exception occurred')
                    logging.exception(err)
                logging.critical(perf.mappings().all())
                perf_rows = [str(row.values()) for row in perf]
                logging.debug('\n'.join(perf_rows))
            try:
                data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='way', params=query_params)
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            if not data.empty:
                logging.info('Found item with simple address search.')
                logging.debug(data.to_string())
                logging.debug('Return with simple address search data: {}'.format(data.iloc[[0]]))
                return data.iloc[[0]]
            else:
                logging.info('Item not found with precise geometry search.')
        if street_query is not None and street_query != '':
            # Using street name for query
            if housenumber_query is not None and housenumber_query != '':
                wnwswh_w = '''
                --- WITH NAME, WITH STREETNAME, WITH HOUSENUMBER
                --- The way selector with street name and with housenumber
                SELECT name, osm_id, {metadata_fields} 950 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                       ST_DistanceSphere(way, point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt,
                       ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                       ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name} {street_query} {housenumber_query}
                    AND ST_DistanceSphere(way, point.geom) < :distance_perfect
                '''
                wnwswh_n = '''
                --- WITH NAME, WITH STREETNAME, WITH HOUSENUMBER
                --- The node selector with street name and with housenumber
                SELECT name, osm_id, {metadata_fields} 950 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                       ST_DistanceSphere(way, point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt,
                       ST_X(planet_osm_point.way) as lon,
                       ST_Y(planet_osm_point.way) as lat
                FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name} {street_query} {housenumber_query}
                    AND ST_DistanceSphere(way, point.geom) < :distance_perfect
                '''
                wnwswh_r = '''
                --- WITH NAME, WITH STREETNAME, WITH HOUSENUMBER
                --- The relation selector with street name and with housenumber
                SELECT name, osm_id, {metadata_fields} 950 AS priority, 'relation' AS node, shop, amenity,
                       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       "addr:conscriptionnumber", ST_DistanceSphere(way, point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt,
                       ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                       ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id < 0 {query_name} {street_query} {housenumber_query}
                    AND ST_DistanceSphere(way, point.geom) < :distance_perfect
                '''
            wnwsnh_w = '''
            --- WITH NAME, WITH STREETNAME, NO HOUSENUMBER
            --- The way selector with street name and without housenumber
            SELECT name, osm_id, {metadata_fields} 970 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                   "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                   ST_DistanceSphere(way, point.geom) as distance, way,
                   ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
            WHERE (({query_type}) AND osm_id > 0 {query_name} {street_query})
                AND ST_DistanceSphere(way, point.geom) < :distance_safe
            '''
            wnwsnh_n = '''
            --- WITH NAME, WITH STREETNAME, NO HOUSENUMBER
            --- The node selector with street name and without housenumber
            SELECT name, osm_id, {metadata_fields} 970 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                   "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                   ST_DistanceSphere(way, point.geom) as distance, way,
                   ST_AsEWKT(way) as way_ewkt,
                   ST_X(planet_osm_point.way) as lon,
                   ST_Y(planet_osm_point.way) as lat
            FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
            WHERE (({query_type}) AND osm_id > 0 {query_name} {street_query})
                AND ST_DistanceSphere(way, point.geom) < :distance_safe
            '''
            wnwsnh_r = '''
            --- WITH NAME, WITH STREETNAME, NO HOUSENUMBER
            --- The relation selector with street name and without housenumber
            SELECT name, osm_id, {metadata_fields} 970 AS priority, 'relation' AS node, shop, amenity,
                   "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                   "addr:conscriptionnumber", ST_DistanceSphere(way, point.geom) as distance, way,
                   ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
            WHERE (({query_type}) AND osm_id < 0 {query_name} {street_query})
                AND ST_DistanceSphere(way, point.geom) < :distance_safe
            '''
        else:
            if housenumber_query is not None and housenumber_query != '':
                wnnswh_w = '''
                --- WITH NAME, NO STREETNAME, WITH HOUSENUMBER
                --- The way selector without street name and with housenumber
                SELECT name, osm_id, {metadata_fields} 970 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                       ST_DistanceSphere(way, point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt,
                       ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                       ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE (({query_type}) AND osm_id > 0 {query_name} {housenumber_query})
                    AND ST_DistanceSphere(way, point.geom) < :distance_safe
                '''
                wnnswh_n = '''
                --- WITH NAME, NO STREETNAME, WITH HOUSENUMBER
                --- The node selector without street name and with housenumber
                SELECT name, osm_id, {metadata_fields} 970 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                       ST_DistanceSphere(way, point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt,
                       ST_X(planet_osm_point.way) as lon,
                       ST_Y(planet_osm_point.way) as lat
                FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE (({query_type}) AND osm_id > 0 {query_name} {housenumber_query})
                    AND ST_DistanceSphere(way, point.geom) < :distance_safe
                '''
                wnnswh_r = '''
                --- WITH NAME, NO STREETNAME, WITH HOUSENUMBER
                --- The relation selector without street name and with housenumber
                SELECT name, osm_id, {metadata_fields} 970 AS priority, 'relation' AS node, shop, amenity,
                       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       "addr:conscriptionnumber",
                       ST_DistanceSphere(way, point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt,
                       ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                       ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id < 0 {query_name} {housenumber_query}
                    AND ST_DistanceSphere(way, point.geom) < :distance_safe
                '''
        # Trying without street name and house number in case when the street name and/or the house not matching at all
        wnnsnh_w = '''
        --- WITH NAME, NO STREETNAME, NO HOUSENUMBER
        --- The way selector without street name and without housenumber
        SELECT name, osm_id, {metadata_fields} 980 AS priority, 'way' AS node, shop, amenity, "addr:housename",
               "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
               ST_DistanceSphere(way, point.geom) as distance, way,
               ST_AsEWKT(way) as way_ewkt,
               ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
               ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
        FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
        WHERE (({query_type}) AND osm_id > 0 {query_name} )
            AND ST_DistanceSphere(way, point.geom) < :distance_safe
        '''
        wnnsnh_n = '''
        --- WITH NAME, NO STREETNAME, NO HOUSENUMBER
        --- The node selector without street name and without housenumber
        SELECT name, osm_id, {metadata_fields} 980 AS priority, 'node' AS node, shop, amenity, "addr:housename",
               "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
               ST_DistanceSphere(way, point.geom) as distance, way,
               ST_AsEWKT(way) as way_ewkt,
               ST_X(planet_osm_point.way) as lon,
               ST_Y(planet_osm_point.way) as lat
        FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
        WHERE (({query_type}) AND osm_id > 0 {query_name} )
            AND ST_DistanceSphere(way, point.geom) < :distance_safe
        '''
        wnnsnh_r = '''
        --- WITH NAME, NO STREETNAME, NO HOUSENUMBER
        --- The relation selector without street name and without housenumber
        SELECT name, osm_id, {metadata_fields} 980 AS priority, 'relation' AS node, shop, amenity,
               "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
               "addr:conscriptionnumber",
               ST_DistanceSphere(way, point.geom) as distance, way,
               ST_AsEWKT(way) as way_ewkt,
               ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
               ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
        FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
        WHERE (({query_type}) AND osm_id < 0 {query_name} )
            AND ST_DistanceSphere(way, point.geom) < :distance_safe
        '''
        nnnsnh_w = '''
            --- NO NAME, NO STREETNAME, NO HOUSENUMBER
            --- The way selector without name and street name
            SELECT name, osm_id, {metadata_fields} 990 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                   "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                   ST_DistanceSphere(way, point.geom) as distance, way,
                   ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
            WHERE (({query_type}) AND osm_id > 0 {query_avoid_name} )
                AND ST_DistanceSphere(way, point.geom) < :distance_unsafe
        '''
        nnnsnh_n = '''
            --- NO NAME, NO STREETNAME, NO HOUSENUMBER
            --- The node selector without name and street name
            SELECT name, osm_id, {metadata_fields} 990 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                   "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                   ST_DistanceSphere(way, point.geom) as distance, way,
                   ST_AsEWKT(way) as way_ewkt,
                   ST_X(planet_osm_point.way) as lon,
                   ST_Y(planet_osm_point.way) as lat
            FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
            WHERE (({query_type}) AND osm_id > 0 {query_avoid_name} )
                AND ST_DistanceSphere(way, point.geom) < :distance_unsafe
        '''
        nnnsnh_r = '''
            --- NO NAME, NO STREETNAME, NO HOUSENUMBER
            --- The relation selector without name street name
            SELECT name, osm_id, {metadata_fields} 990 AS priority, 'relation' AS node, shop, amenity,
                   "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                   "addr:conscriptionnumber",
                   ST_DistanceSphere(way, point.geom) as distance, way,
                   ST_AsEWKT(way) as way_ewkt,
                   ST_X(ST_PointOnSurface(planet_osm_polygon.way)) as lon,
                   ST_Y(ST_PointOnSurface(planet_osm_polygon.way)) as lat
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
            WHERE (({query_type}) AND osm_id < 0 {query_avoid_name} )
                AND ST_DistanceSphere(way, point.geom) < :distance_unsafe
        '''
        query_text = []
        if 'wnwswh_w' in locals(): query_arr.append(wnwswh_w)
        if 'wnwswh_n' in locals(): query_arr.append(wnwswh_n)
        if 'wnwswh_r' in locals(): query_arr.append(wnwswh_r)

        if 'wnwsnh_w' in locals(): query_arr.append(wnwsnh_w)
        if 'wnwsnh_n' in locals(): query_arr.append(wnwsnh_n)
        if 'wnwsnh_r' in locals(): query_arr.append(wnwsnh_r)

        if 'wnnsnh_w' in locals(): query_arr.append(wnnsnh_w)
        if 'wnnsnh_n' in locals(): query_arr.append(wnnsnh_n)
        if 'wnnsnh_r' in locals(): query_arr.append(wnnsnh_r)

        if 'wnnswh_w' in locals(): query_arr.append(wnnswh_w)
        if 'wnnswh_n' in locals(): query_arr.append(wnnswh_n)
        if 'wnnswh_r' in locals(): query_arr.append(wnnswh_r)

        if 'nnnsnh_w' in locals(): query_arr.append(nnnsnh_w)
        if 'nnnsnh_n' in locals(): query_arr.append(nnnsnh_n)
        if 'nnnsnh_r' in locals(): query_arr.append(nnnsnh_r)

        #  Create smaller queries Issue #100
        if config.get_database_enable_huge_query():
            query_text = ['UNION ALL'.join(query_arr) + ' ORDER BY priority ASC, distance ASC;']
        else:
            query_text = [i + ' ORDER BY priority ASC, distance ASC;' for i in query_arr]
        stage = 0
        for q in query_text:
            stage += 1
            try:
                query = sqlalchemy.text(q.format(query_type=query_type, query_name=query_name,
                                             query_avoid_name=query_avoid_name, metadata_fields=metadata_fields,
                                             street_query=street_query, city_query=city_query,
                                             housenumber_query=housenumber_query))
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            #  Make EXPLAIN ANALYZE of long queries configurable with issue #99
            if config.get_database_enable_analyze() is True:
                try:
                    perf_query = sqlalchemy.text('EXPLAIN ANALYZE ' + q.format(query_type=query_type, query_name=query_name,
                                                                        query_avoid_name=query_avoid_name,
                                                                        metadata_fields=metadata_fields,
                                                                        street_query=street_query,
                                                                        city_query=city_query,
                                                                        housenumber_query=housenumber_query))
                    perf = self.session.execute(perf_query, query_params)
                except Exception as err:
                    logging.warning('Exception occurred')
                    logging.exception(err)
                #logging.critical(perf.mappings().all())
                perf_rows = [str(row.values()) for row in perf.mappings().all()]
                logging.debug('\n'.join(perf_rows))
            try:
                data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='way', params=query_params)
            except Exception as err:
                logging.warning('Exception occurred')
                logging.exception(err)
            logging.debug(str(query))
            if not data.empty:
                logging.info('Found item in {} stage without precise geometry search.'.format(stage))
                logging.debug(data.to_string())
                logging.debug('Return without precise geometry search data: {}'.format(data.iloc[[0]]))
                return data.iloc[[0]]
            else:
                logging.info('Item not found in stage {}.'.format(stage))
        logging.info('Item not found at all.')
        try:
            self.session.commit()
            self.session.close()
        except Exception as err:
            logging.warning('Exception occurred')
            logging.exception(err)
        return None

    def query_osm_building_poi_gpd(self, lon, lat, city, postcode, street_name='', housenumber='',
                                   in_building_percentage=0.50, distance=60):
        '''
        Looking for a building (actually a way) around the POI node with same address with within a preconfigured distance.
        Actually this method helps to find a building for a single node.
        :param lon: Longitude of POI node type coordinate.
        :param lat: Latitude of POI node type coordinate.
        :param: city: Name of city where the POI node is.
        :param: postcode: Postcode of area where the POI node is.
        :param: street_name: Name of street where the POI node is.
        :param: housenumber: House number of street where the POI node is.
        :param: in_building_percentage: In building line argument is a float8 between 0 and 1 representing fraction of
          total linestring length the point has to be located.
          Documentation: https://postgis.net/docs/ST_LineInterpolatePoint.html
        :param: distance: Look buildings around the POI node within this radius (specified in meter).
        :return: A new node within the building with same address.
        '''
        buffer = 10
        # When we got all address parts, then we should try to fetch only one coordinate pair of building geometry
        if street_name is not None and street_name != '' and housenumber is not None and housenumber != '':
            street_query = ' AND LOWER(TEXT("addr:street")) = LOWER(TEXT(:street_name))'
            housenumber_query = ' AND LOWER(TEXT("addr:housenumber")) = LOWER(TEXT(:housenumber))'
        else:
            return None
        query = sqlalchemy.text('''
            --- The building selector based on POI node distance and
            --- city, postcode, street name and housenumber of building
            --- Make a line that connects the building and POI coords, using one point from union
            SELECT name, building, osm_id, 'way' AS node, "addr:housename",
                "addr:housenumber", "addr:postcode", "addr:city", "addr:street", "addr:conscriptionnumber",
                 ST_Y(ST_PointOnSurface(way)) as lat_in_b,
                 ST_X(ST_PointOnSurface(way)) as lon_in_b,
                 ST_Y(ST_LineInterpolatePoint(ST_GeometryN(ST_Intersection(way,
                 ST_MakeLine(ST_PointOnSurface(way), point.geom)), 1), :ibp)) as lat,
                 ST_X(ST_LineInterpolatePoint(ST_GeometryN(ST_Intersection(way,
                 ST_MakeLine(ST_PointOnSurface(way), point.geom)), 1), :ibp)) as lon,
                ST_DistanceSphere(way, point.geom) as distance, way,
                ST_PointOnSurface(way) in_building, ST_AsEWKT(way) as way_ewkt,
                ST_AsEWKT(ST_PointOnSurface(way)) in_building_ewkt
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
            WHERE building <> '' AND osm_id > 0 AND ST_DistanceSphere(way, point.geom) < :distance
                {street_query} {housenumber_query}
            ORDER BY distance ASC LIMIT 1'''.format(street_query=street_query, housenumber_query=housenumber_query))
        data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='way', params={'lon': lon, 'lat': lat,
                                                                                         'distance': distance,
                                                                                         'buffer': buffer,
                                                                                         'street_name': street_name,
                                                                                         'housenumber': housenumber,
                                                                                         'ibp': in_building_percentage})
        if data.empty:
            return None
        else:
            return data

    def query_poi_in_water(self, lon, lat):
        distance = 1
        try:
            query = sqlalchemy.text('''
            SELECT * FROM
              (SELECT osm_id, way, ST_DistanceSphere(way, point.geom) as distance
              FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon, :lat), 4326) as geom) point
              WHERE (water IS NOT NULL OR waterway IS NOT NULL)
              ORDER BY distance ASC LIMIT 1) AS geo
            WHERE geo.distance < :distance
              ''')
            data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='way', params={'lon': lon,
                                                                                                   'lat': lat,
                                                                                             'distance': distance})
            return data
        except Exception as err:
            logging.warning('Exception occurred')
            logging.exception(err)

    def query_name_road_around(self, lon, lat, name='', with_metadata=True, mode='all',
                               similarity_threshold: float = 0.49, levenshtein_threshold: int = 5):
        '''
        Search for road with name specified around the lon, lat point location in OpenStreetMap database based on
        within preconfigured distance
        :param lon: Longitude parameter. Looking for roads around this geom.
        :param lat: Latitude parameter. Looking for roads around this geom.
        :param name: Name of the road. Search OpenStreetMap "highway" tagged ways with this name tag.
        :param with_metadata: Query OpenStreetMap metadata information
        :param mode: Looking for name, metaphone or both
        :return: GeoDataFrame of distance ordered result.
        '''
        logging.debug(f'Looking for streets around with same name, using method: {mode} ...')
        try:
            distance = config.get_geo_default_poi_road_distance()
            if with_metadata is True:
                metadata_fields = ' osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, '
            else:
                metadata_fields = ''
            # Looking for way (road)
            if mode == 'all':
                name_query = '("name" = :name OR dmetaphone(name) = dmetaphone(:name) OR similarity(name, :name) >= :similarity_threshold OR levenshtein(name, :name) < :levenshtein_threshold)'
            if mode == 'both':
                name_query = '("name" = :name OR dmetaphone(name) = dmetaphone(:name))'
            elif mode == 'name':
                name_query = '("name" = :name)'
            elif mode == 'metaphone':
                name_query = 'dmetaphone(name) = dmetaphone(:name)'
            elif mode == 'similarity':
                name_query = 'similarity(name, :name) >= :similarity_threshold'
            elif mode == 'levenshtein':
                name_query = 'levenshtein(name, :name) < :levenshtein_threshold'
            else:
                name_query = '("name" = :name OR dmetaphone(name) = dmetaphone(:name) OR similarity(name, :name) >= :similarity_threshold OR levenshtein(name, :name) < :levenshtein_threshold)'
        except Exception as err:
            logging.warning('Exception occurred')
            logging.exception(err)
        try:
            query = sqlalchemy.text('''
              SELECT * FROM
                (SELECT name, osm_id, highway, similarity(name, :name), dmetaphone(name) as dmp_found, dmetaphone(:name) as dmp_original, levenshtein(name, :name), {metadata_fields}
                  ST_DistanceSphere(way, point.geom) as distance, way, ST_AsEWKT(way) as way_ewkt
                    FROM planet_osm_line, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE "highway" is not NULL
                  AND {name_query}
                ORDER BY similarity DESC, distance ASC) AS geo
              WHERE geo.distance < :distance
              LIMIT 5
              '''.format(metadata_fields=metadata_fields, name_query=name_query))
            data = gpd.GeoDataFrame.from_postgis(query, self.connection(), geom_col='way',
                                                 params={'lon': lon, 'lat': lat, 'distance': distance,
                                                 'name': name, 'similarity_threshold': similarity_threshold,
                                                 'levenshtein_threshold': levenshtein_threshold})
        except Exception as err:
            logging.warning('Exception occurred')
            logging.exception(err)
        try:
            data.sort_values(by=['levenshtein'])
        except Exception as err:
            logging.warning('Exception occurred')
            logging.exception(err)
        logging.debug({'lon': lon, 'lat': lat, 'distance': distance, 'name': name,
                       'similarity_threshold': similarity_threshold, 'levenshtein_threshold': levenshtein_threshold})
        logging.debug(f'Returned data:')
        logging.debug(f'{data.to_string()}')

        return data
