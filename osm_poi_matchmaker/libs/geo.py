# -*- coding: utf-8 -*-

try:
    from geoalchemy2 import WKTElement
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


def geom_point(latitude, longitude, projection):
    if latitude is not None and longitude is not None:
        return WKTElement('POINT({} {})'.format(latitude, longitude), srid=projection)
    else:
        return None


def check_geom(latitude, longitude, proj=config.get_geo_default_projection()):
    if (latitude is not None and latitude != '') and (longitude is not None and longitude != ''):
        return geom_point(latitude, longitude, proj)
    else:
        return None
