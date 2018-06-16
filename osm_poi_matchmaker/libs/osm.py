# -*- coding: utf-8 -*-
try:
    import sqlalchemy
    import geopandas as gpd
    from OSMPythonTools.overpass import Overpass
    from OSMPythonTools.nominatim import Nominatim
    from OSMPythonTools.overpass import overpassQueryBuilder
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


def get_area_id(area):
    # Query Nominatom
    nominatim = Nominatim()
    return nominatim.query(area).areaId()


def query_overpass(area_id, query_statement, element_type='node'):
    # Query Overpass based on area
    overpass = Overpass()
    query = overpassQueryBuilder(area=area_id, elementType=element_type, selector=query_statement)
    return overpass.query(query)

def query_osm_postcode_gpd(session, lon, lat):
    if lat is None or lat == '' or lon == '' or lon is None: return None
    query = sqlalchemy.text('''
        SELECT name
        FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lat,:lon),4326) as geom) point
        WHERE boundary='postal_code' and ST_Contains(way, ST_Transform(point.geom,3857)) ORDER BY name LIMIT 1;''')
    data = session.execute(query, {'lon':lon, 'lat':lat}).first()
    if data is None: return None
    row = dict(zip(data.keys(), data))
    return int(row['name'].split(' ')[0]) if row['name'].split(' ')[0] is not None else None

def query_postcode_osm_external(prefer_osm, session, lon, lat, postcode_ext):
    if prefer_osm is False and postcode_ext is not None:
        return postcode_ext
    query_postcode = query_osm_postcode_gpd(session, lon, lat)
    if prefer_osm is True and query_postcode is not None:
        return query_postcode
    elif prefer_osm is True and query_postcode is None:
        return postcode_ext
