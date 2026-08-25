# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import sqlalchemy
    from sqlalchemy.orm import scoped_session, sessionmaker
    import geopandas as gpd
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class POIQC:
    """Data-quality checks run on a single POI's coordinates/address, collecting
    'good'/'bad' quality labels (stored as POI_address.poi_good/poi_bad and shown as
    a comment in the generated OSM XML - see file_output.py)."""

    def __init__(self, db, lon, lat, opening_hours=None, street=None):
        """Run every quality check immediately; results are read via process().

        Args:
            db: A POIBase instance (used for the underlying spatial queries).
            lon (float): POI longitude.
            lat (float): POI latitude.
            opening_hours (str | None): Proposed opening_hours value, if any -
                enables the custom-opening-hours check when given.
            street (str | None): Proposed street name, used by the road-nearby checks.
        """
        self.__db = db
        self.__lon = lon
        self.__lat = lat
        self.__good = []
        self.__bad = []
        self.__distance = 1
        self.__opening_hours = opening_hours
        self.__street = street
        self.__check()

    def __check(self):
        """Run all quality checks, appending their labels to self.__good/__bad."""
        self.__is_in_water()
        self.__is_name_road_around()
        self.__is_name_metaphone_road_around()
        if self.__opening_hours is not None:
            self.__custom_opening_hours()

    def process(self):
        """Return the collected quality labels.

        Returns:
            tuple[list[str], list[str]]: (good labels, bad labels).
        """
        return self.__good, self.__bad

    def __is_in_water(self):
        """Flag 'in_water' if the POI's coordinates fall on an OSM water polygon."""
        data = self.__db.query_poi_in_water(self.__lon, self.__lat)
        if data.empty:
            self.__good.append('not_in_water')
        else:
            self.__bad.append('in_water')

    def __custom_opening_hours(self):
        """Flag 'custom_opening_hours' if the opening_hours value uses constructs
        (comment, status, dawn/sunrise/sunset/dusk) that can't be handled
        automatically, otherwise flag 'standard_opening_hours'."""
        if self.__opening_hours is not None:
            if 'comment' in self.__opening_hours or 'status' in self.__opening_hours or \
               'dawn' in self.__opening_hours or 'sunrise' in self.__opening_hours or \
               'sunset' in self.__opening_hours or 'dusk' in self.__opening_hours:
                self.__bad.append('custom_opening_hours')
            else:
                self.__good.append('standard_opening_hours')

    def __is_name_road_around(self):
        """Flag whether a road named self.__street exists near the POI (exact name
        match)."""
        data = self.__db.query_name_road_around(self.__lon, self.__lat, self.__street, True, 'name')
        if data.empty:
            self.__bad.append('street_is_not_around')
        else:
            self.__good.append('street_is_around')

    def __is_name_metaphone_road_around(self):
        """Same as __is_name_road_around(), but matching by metaphone (phonetic)
        similarity instead of exact name - catches minor spelling differences."""
        data = self.__db.query_name_road_around(self.__lon, self.__lat, self.__street, True, 'metaphone')
        if data.empty:
            self.__bad.append('street_metaphone_is_not_around')
        else:
            self.__good.append('street_metaphone_is_around')
