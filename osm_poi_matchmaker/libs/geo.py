# -*- coding: utf-8 -*-

try:
    import logging
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


def check_hu_boundary(latitude, longitude):
    if (latitude is not None and latitude != '') and (longitude is not None and longitude != ''):
        # This is a workaround because original datasource may contains swapped lat / lon parameters
        if float(latitude) < 44:
            logging.warning(
                'Latitude-longitude replacement. Originally was: latitude: {}, longitude: {}.'.format(latitude,
                                                                                                      longitude))
            longitude, latitude = latitude, longitude
        # Another workaround to insert missing decimal point
        if float(longitude) > 200:
            longitude = '{}.{}'.format(longitude[:2], longitude[3:])
        if float(latitude) > 200:
            latitude = '{}.{}'.format(latitude[:2], latitude[3:])
        return latitude, longitude
    else:
        return None, None
