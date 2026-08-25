# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import re
    from geoalchemy2 import WKTElement
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

PATTERN_COORDINATE = re.compile(r'[\d]{1,3}.[\d]{2,5}')


def geom_point(latitude, longitude, projection):
    """Build a GeoAlchemy2 WKT point element from raw coordinates.

    Args:
        latitude: Latitude value (used as the WKT point's Y ordinate).
        longitude: Longitude value (used as the WKT point's X ordinate).
        projection: SRID for the point, e.g. config.get_geo_default_projection().

    Returns:
        WKTElement | None: 'POINT(latitude longitude)' with the given SRID, or None
        if either coordinate is missing.
    """
    if latitude is not None and longitude is not None:
        return WKTElement('POINT({} {})'.format(latitude, longitude), srid=projection)
    else:
        return None


def check_geom(latitude, longitude, proj=config.get_geo_default_projection()):
    """
    Basic check of latitude and longitude geom point
    Are both coordinates are exist and extract only the right format

    :param latitude: Coordinate latitude part of geom
    :param longitude: Coordinate longitude part of geom
    :param proj: Projection of geom
    :return: Validated coordinates or None on error
    """
    if (latitude is not None and latitude != '') and (longitude is not None and longitude != ''):
        if not isinstance(latitude, float) and not isinstance(latitude, int):
            la = PATTERN_COORDINATE.search(latitude.replace(',', '.').strip())
            try:
                if la is not None:
                    lat = la.group()
                else:
                    return None
            except (AttributeError, IndexError) as e:
                logging.error('%s;%s', latitude, longitude)
                logging.error(e)
                logging.exception('Exception occurred')

                return None
        else:
            lat = latitude
        if not isinstance(longitude, float) and not isinstance(longitude, int):
            lo = PATTERN_COORDINATE.search(longitude.replace(',', '.').strip())
            try:
                if lo is not None:
                    lon = lo.group()
                else:
                    return None
            except (AttributeError, IndexError) as e:
                logging.error('%s;%s', latitude, longitude)
                logging.error(e)
                logging.exception('Exception occurred')

                return None
        else:
            lon = longitude
        return geom_point(lat, lon, proj)
    else:
        return None


def check_hu_boundary(latitude, longitude):
    """Sanity-check and auto-correct a coordinate pair for Hungary. Called by nearly
    every data provider as `self.data.lat, self.data.lon = check_hu_boundary(...)`.

    Handles two data-source quirks seen in the wild:
      - Swapped lat/lon: Hungary's latitude is always < 50, longitude < 23, so if
        latitude >= 44 doesn't hold, the two values are swapped.
      - Missing decimal point: if a coordinate is > 200 (e.g. '473521' meant to be
        '47.3521'), a decimal point is inserted after the first 2 digits.

    Args:
        latitude: Raw latitude value (str, int or float; '' and 0.0 count as missing).
        longitude: Raw longitude value, same rules as latitude.

    Returns:
        tuple: (latitude, longitude) after any swap/decimal-point correction, or
        (None, None) if either input was missing.
    """
    if (latitude is not None and latitude != '' and latitude != 0.0) and (
            longitude is not None and longitude != '' and longitude != 0.0):
        # This is a workaround because original datasource may contains swapped lat / lon parameters
        if float(latitude) < 44:
            logging.warning(
                'Latitude-longitude replacement. Originally was: latitude: %s, longitude: %s.',
                latitude, longitude)
            longitude, latitude = latitude, longitude
        # Another workaround to insert missing decimal point
        if float(longitude) > 200:
            longitude = '{}.{}'.format(longitude[:2], longitude[3:])
            if longitude.count('.') > 1:
                lon_tmp = longitude.split('.')
                longitude = '.'.join(lon_tmp[0:1])
        if float(latitude) > 200:
            latitude = '{}.{}'.format(latitude[:2], latitude[3:])
            if latitude.count('.') > 1:
                lat_tmp = latitude.split('.')
                latitude = '.'.join(lat_tmp[0:1])
        return latitude, longitude
    else:
        return None, None
