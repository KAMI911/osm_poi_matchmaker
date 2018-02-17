# -*- coding: utf-8 -*-

try:
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
