# -*- coding: utf-8 -*-
try:
    import traceback
    import geopandas as gpd
    import pandas as pd
    import sqlalchemy
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.dao.data_structure import Base, OSM_object_type

except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


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
    def pool(self):
        return self.engine

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

    def count_all_gpd(self, table):
        '''
        Load all POI data from SQL that contains gometry
        :param table: Name of table where POI data is stored
        :return: Full table with poi_lat and poi_long fileds read from SQL database table
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
                return data.to_dict('records')
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

    def query_osm_shop_poi_gpd(self, lon, lat, ptype='shop', name='', with_metadata=True):
        '''
        Search for POI in OpenStreetMap database based on POI type and geom within preconfigured distance
        :param lon:
        :param lat:
        :param ptype:
        :parm name:
        :parm with_metadata:
        :return:
        '''
        buffer = 50
        if ptype == 'shop':
            query_type = "shop='convenience' OR shop='supermarket'"
            distance = config.get_geo_shop_poi_distance()
        elif ptype == 'fuel':
            query_type = "amenity='fuel'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'bank':
            query_type = "amenity='bank'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'atm':
            query_type = "amenity='atm'"
            distance = config.get_geo_amenity_atm_poi_distance()
        elif ptype == 'post_office':
            query_type = "amenity='post_office'"
            distance = config.get_geo_amenity_post_office_poi_distance()
        elif ptype == 'vending_machine':
            query_type = "amenity='vending_machine'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'pharmacy':
            query_type = "amenity='pharmacy'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'chemist':
            query_type = "shop='chemist'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'bicycle_rental':
            query_type = "amenity='bicycle_rental'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'vending_machine_cheques':
            query_type = "amenity='vending_machine' AND vending='cheques'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'vending_machine_parcel_pickup':
            query_type = "amenity='vending_machine' AND vending='parcel_pickup'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'vending_machine_parcel_mail_in':
            query_type = "amenity='vending_machine' AND vending='parcel_mail_in'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'vending_machine_parcel_pickup_and_mail_in':
            query_type = "amenity='vending_machine' AND vending='parcel_pickup;parcel_mail_in'"
            distance = config.get_geo_default_poi_distance()
        elif ptype == 'vending_machine_parking_tickets':
            query_type = "amenity='vending_machine' AND vending='parking_tickets'"
            distance = config.get_geo_default_poi_distance()
        if name is not '':
            query_name = ' AND name ~* :name'
            buffer += 600
            distance += 800
        else:
            query_name = ''
        if with_metadata is True:
            metadata_fields = ' osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, '
        else:
            metadata_fields = ''
        # Looking for way (building)
        query = sqlalchemy.text('''
            SELECT name, osm_id, {metadata_fields} 'way' AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
            WHERE ({query_type}) AND osm_id > 0 {query_name}
                AND ST_DWithin(ST_Buffer(way,:buffer),ST_Transform(point.geom,3857), :distance)
            ORDER BY distance ASC;'''.format(query_type=query_type, query_name=query_name,
                                             metadata_fields=metadata_fields))
        data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat,
                                                                                         'distance': distance,
                                                                                         'name': '.*{}.*'.format(name),
                                                                                         'buffer': buffer})
        # Looking for node
        query = sqlalchemy.text('''
            SELECT name, osm_id, {metadata_fields} 'node' AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon, ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
            FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
            WHERE ({query_type}) AND osm_id > 0 {query_name}
                AND ST_DWithin(way,ST_Transform(point.geom,3857), :distance)
            ORDER BY distance ASC;'''.format(query_type=query_type, query_name=query_name,
                                             metadata_fields=metadata_fields))
        data2 = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way',
                                              params={'lon': lon, 'lat': lat, 'distance': distance,
                                                      'name': '.*{}.*'.format(name)})
        query = sqlalchemy.text('''
            SELECT name, osm_id, {metadata_fields} 'relation' AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
            WHERE ({query_type}) AND osm_id < 0 {query_name}
                AND ST_DWithin(ST_Buffer(way,:buffer),ST_Transform(point.geom,3857), :distance)
            ORDER BY distance ASC;'''.format(query_type=query_type, query_name=query_name,
                                             metadata_fields=metadata_fields))
        data3 = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat,
                                                                                          'distance': distance,
                                                                                          'name': '.*{}.*'.format(name),
                                                                                          'buffer': buffer})
        if data.empty and data2.empty and data3.empty:
            return None
        else:
            data = data.append(data2)
            data = data.append(data3)
            data.sort_values(by=['distance'])
            return data

    def query_poi_in_water(self, lon, lat):
        distance = 1
        try:
            query = sqlalchemy.text('''
            SELECT osm_id, way, ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance
            FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon, :lat), 4326) as geom) point
            WHERE(water IS NOT NULL OR waterway IS NOT NULL)
              AND ST_DWithin(way, ST_Transform(point.geom, 4326), :distance)
            ORDER BY distance ASC ''')
            data = gpd.GeoDataFrame.from_postgis(query, self.engine, geom_col='way', params={'lon': lon, 'lat': lat,
                                                                                             'distance': distance})
            return data
        except Exception as err:
            traceback.print_exc()
            print(err)
