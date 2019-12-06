# -*- coding: utf-8 -*-
try:
    import traceback
    import logging
    import geopandas as gpd
    import pandas as pd
    import sqlalchemy
    import time
    from math import isnan
    from utils import config, poitypes
    from dao.data_structure import Base, OSM_object_type
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


class POIBase:
    """Represents the full database.

    :param db_conection: Either a sqlalchemy database url or a filename to be used with sqlite.

    """

    def __init__(self, db_connection, retry_counter=100, retry_sleep=30):
        reco = 0 # Actual retry counter
        self.db_retry_counter = retry_counter
        self.db_retry_sleep = retry_sleep
        self.db_connection = db_connection
        self.db_filename = None
        if '://' not in db_connection:
            self.db_connection = 'sqlite:///%s' % self.db_connection
        if self.db_connection.startswith('sqlite'):
            self.db_filename = self.db_connection
        try:
            self.engine = sqlalchemy.create_engine(self.db_connection, client_encoding='utf8', echo=False)
        except psycopg2.OperationalError as e:
            if self.retry_counter >= reco:
                logging.error('Cannot connect to database with {} connection string'.format(self.db_connection))
            else:
                logging.error('Cannot connect to the database. It will retry within {} seconds. [{}/{}]'.format(self.db_retry_sleep, reco, self.db_retry_counter))
                time.sleep(self.db_retry_sleep)
                self.engine = sqlalchemy.create_engine(self.db_connection, echo=False)
                self.db_retry_counter += 1
        self.Session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    @property
    def pool(self):
        return self.engine

    @property
    def session(self):
        return self.Session()

    def __del__(self):
        self.session.close()
        self.engine.dispose()

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

    def query_all_gpd_in_order(self, table):
        '''
        Load all POI data from SQL that contains gometry and ordered by poi_common_id and postcode
        :param table: Name of table where POI data is stored
        :return: Full table with poi_lat and poi_long fileds read from SQL database table
        '''
        query = sqlalchemy.text('select * from {} where poi_geom is not NULL order by poi_common_id ASC, poi_postcode ASC'.format(table))
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='poi_geom')
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
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='poi_geom')
        return data


    def query_from_cache(self, node_id, object_type):
        if node_id > 0:
            query = sqlalchemy.text('select * from poi_osm_cache where osm_id = :node_id and osm_object_type = :object_type limit 1')
            data = pd.read_sql(query, self.engine, params={'node_id': int(node_id), 'object_type': object_type.name})
            if not data.values.tolist():
                return None
            else:
                return data.to_dict('records')[0]
        else:
            return None

    def query_ways_nodes(self, way_id):
        if way_id > 0:
            query = sqlalchemy.text('select nodes from planet_osm_ways where id = :way_id limit 1')
            data = pd.read_sql(query, self.engine, params={'way_id': int(way_id)})
            return data.values.tolist()[0][0]
        else:
            return None

    def query_relation_nodes(self, relation_id):
        query = sqlalchemy.text('select members from planet_osm_rels where id = :relation_id limit 1')
        data = pd.read_sql(query, self.engine, params={'relation_id': int(abs(relation_id))})
        return data.values.tolist()[0][0]

    def query_osm_shop_poi_gpd(self, lon, lat, ptype='shop', name='', street_name='', housenumber='', distance_perfect=None, distance_safe=None, distance_unsafe=None, with_metadata=True):
        '''
        Search for POI in OpenStreetMap database based on POI type and geom within preconfigured distance
        :param lon:
        :param lat:
        :param ptype:
        :parm name:
        :parm with_metadata:
        :return:
        '''
        buffer = 10
        query_type, distance = poitypes.getPOITypes(ptype)
        # If we have PO common definied unsafe search radius distance, then use it (or use defaults specified above)
        if name is not '':
            query_name = ' AND (LOWER(name) ~* LOWER(:name) OR LOWER(brand) ~* LOWER(:name))'
            # If we have PO common definied safe search radius distance, then use it (or use defaults specified above)
            distance = float(config.get_geo_default_poi_distance()) if isnan(float(distance_safe)) \
                else float(distance_safe)
            distance_perfect = float(config.get_geo_default_poi_perfect_distance()) if isnan(float(distance_perfect)) \
                else float(distance_perfect)
        else:
            query_name = ''
            if not isnan(distance_unsafe):
                distance = distance_unsafe
        if with_metadata is True:
            metadata_fields = ' osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, '
        else:
            metadata_fields = ''
        if street_name is not None and street_name != '':
            street_query = ' AND LOWER("addr:street") = LOWER(:street_name)'
        else:
            street_query = ''
        if housenumber is not None and housenumber != '':
            housenumber_query = ' AND LOWER("addr:housenumber") = LOWER(:housenumber)'
        else:
            housenumber_query = ''
        # Looking for way (building)
        if street_query is not None and street_query != '':
            # Using street name for query
            query = sqlalchemy.text('''
                --- The way selector with street name and housenumber
                SELECT name, osm_id, {metadata_fields} 970 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name} {street_query} {housenumber_query}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance_perfect
                UNION ALL
                --- The node selector with street name and housenumber
                SELECT name, osm_id, {metadata_fields} 970 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon,
                       ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
                FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name} {street_query} {housenumber_query}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance_perfect
                UNION ALL
                --- The relation selector with street name and housenumber
                SELECT name, osm_id, {metadata_fields} 970 AS priority, 'relation' AS node, shop, amenity,
                       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id < 0 {query_name} {street_query} {housenumber_query}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance_perfect
                UNION ALL
                --- The way selector with street name
                SELECT name, osm_id, {metadata_fields} 980 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name} {street_query}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :street_distance
                UNION ALL
                --- The node selector with street name
                SELECT name, osm_id, {metadata_fields} 980 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon,
                       ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
                FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name} {street_query}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :street_distance
                UNION ALL
                --- The relation selector with street name
                SELECT name, osm_id, {metadata_fields} 980 AS priority, 'relation' AS node, shop, amenity,
                       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id < 0 {query_name} {street_query}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :street_distance
                UNION ALL
                --- The way selector without street name
                SELECT name, osm_id, {metadata_fields} 990 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                UNION ALL
                --- The node selector without street name
                SELECT name, osm_id, {metadata_fields} 990 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon,
                       ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
                FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                UNION ALL
                --- The relation selector without street name
                SELECT name, osm_id, {metadata_fields} 990 AS priority, 'relation' AS node, shop, amenity,
                       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id < 0 {query_name}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                ORDER BY priority ASC, distance ASC;'''.format(query_type=query_type, query_name=query_name,
                                                               metadata_fields=metadata_fields,
                                                               street_query=street_query,
                                                               housenumber_query=housenumber_query))
        else:
            # Not using street name for query
            query = sqlalchemy.text('''
                SELECT name, osm_id, {metadata_fields} 998 AS priority, 'way' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                UNION ALL
                SELECT name, osm_id, {metadata_fields} 999 AS priority, 'node' AS node, shop, amenity, "addr:housename",
                       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon,
                       ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
                FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id > 0 {query_name}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                UNION ALL
                SELECT name, osm_id, {metadata_fields} 997 AS priority, 'relation' AS node, shop, amenity,
                       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                       ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
                FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE ({query_type}) AND osm_id < 0 {query_name}
                    AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                ORDER BY priority ASC, distance ASC;'''.format(query_type=query_type, query_name=query_name,
                                                           metadata_fields=metadata_fields))
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat,
                                                                                          'distance': distance,
                                                                                          'street_distance': 850,
                                                                                          'distance_perfect': distance_perfect,
                                                                                          'name': '.*{}.*'.format(name),
                                                                                          'buffer': buffer,
                                                                                          'street_name': street_name,
                                                                                          'housenumber': housenumber})
        if data.empty:
            return None
        else:
            return data


    def query_osm_building_poi_gpd(self, lon, lat, city, postcode, street_name='', housenumber='', in_building_percentage=0.50, distance=60):
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
            street_query = ' AND LOWER("addr:street") = LOWER(:street_name)'
            housenumber_query = ' AND LOWER("addr:housenumber") = LOWER(:housenumber)'
        else:
            return None
        query = sqlalchemy.text('''
            --- The building selector based on POI node distance and
            --- city, postcode, street name and housenumber of building
            --- Make a line that connects the building and POI coords, using one point from union
            SELECT name, building, osm_id, 'way' AS node, "addr:housename",
                "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
                 ST_Y(ST_Transform(ST_PointOnSurface(way),4326)) as lat_in_b,
                 ST_X(ST_Transform(ST_PointOnSurface(way),4326)) as lon_in_b,

                 ST_Y(ST_LineInterpolatePoint(ST_GeometryN(ST_Intersection(ST_Transform(way, 4326),
                 ST_MakeLine(ST_PointOnSurface(ST_Transform(way, 4326)), point.geom)), 1), :ibp)) as lat,
                 ST_X(ST_LineInterpolatePoint(ST_GeometryN(ST_Intersection(ST_Transform(way, 4326),
                 ST_MakeLine(ST_PointOnSurface(ST_Transform(way, 4326)), point.geom)), 1), :ibp)) as lon,

                ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,
                ST_PointOnSurface(way) in_building, ST_AsEWKT(ST_Transform(way, 4326)) as way_ewkt,
                ST_AsEWKT(ST_PointOnSurface(ST_Transform(way, 4326))) in_building_ewkt
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
            WHERE building <> '' AND osm_id > 0 AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                {street_query} {housenumber_query}
            ORDER BY distance ASC LIMIT 1'''.format(street_query=street_query, housenumber_query=housenumber_query))
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat,
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
            SELECT osm_id, way, ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon, :lat), 4326) as geom) point
            WHERE(water IS NOT NULL OR waterway IS NOT NULL)
              AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
            ORDER BY distance ASC ''')
            data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat,
                                                                                             'distance': distance})
            return data
        except Exception as err:
            logging.error(err)
            logging.error(traceback.print_exc())

    def query_name_road_around(self, lon, lat, name='', with_metadata=True):
        '''
        Search for road wit name specified around the lon, lat point location in OpenStreetMap database based on within preconfigured distance
        :param lon:
        :param lat:
        :parm name:
        :parm with_metadata:
        :return:
        '''
        try:
            distance = config.get_geo_default_poi_road_distance()
            if with_metadata is True:
                metadata_fields = ' osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, '
            else:
                metadata_fields = ''
            # Looking for way (road)
            query = sqlalchemy.text('''
                SELECT name, osm_id, highway, {metadata_fields} ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way, ST_AsEWKT(way) as way_ewkt
                FROM planet_osm_line, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
                WHERE "name" = :name AND "highway" is not NULL
                AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                ORDER BY distance ASC;
                '''.format(metadata_fields=metadata_fields))
            data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat,
                                                                                             'distance': distance,
                                                                                             'name': '{}'.format(
                                                                                                 name)})
            data.sort_values(by=['distance'])
            return data
        except Exception as err:
            logging.error(err)
            logging.error(traceback.print_exc())


    def query_name_metaphone_road_around(self, lon, lat, name='', with_metadata=True):
        '''
        Search for road wit metaphone name (as pronounced) specified around the lon, lat point location in OpenStreetMap database based on within preconfigured distance
        :param lon:
        :param lat:
        :parm name:
        :parm with_metadata:
        :return:
        '''
        try:
            distance = config.get_geo_default_poi_road_distance()
            if with_metadata is True:
                metadata_fields = ' osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, '
            else:
                metadata_fields = ''
            # Looking for way (road)
            query = sqlalchemy.text('''
                SELECT name, osm_id, highway, {metadata_fields} ST_DistanceSphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt
                FROM planet_osm_line, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
                WHERE dmetaphone(name) = dmetaphone(:name) AND highway is not NULL
                AND ST_DistanceSphere(ST_Transform(way, 4326), point.geom) < :distance
                ORDER BY distance ASC;
                '''.format(metadata_fields=metadata_fields))
            data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat,
                                                                                             'distance': distance,
                                                                                             'name': '{}'.format(
                                                                                                 name)})
            data.sort_values(by=['distance'])
            return data
        except Exception as err:
            logging.error(err)
            logging.error(traceback.print_exc())
