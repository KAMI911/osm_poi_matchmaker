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

    def __init__(self, db, lon, lat, opening_hours=None, street=None):
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
        self.__is_in_water()
        self.__is_name_road_around()
        self.__is_name_metaphone_road_around()
        if self.__opening_hours is not None:
            self.__custom_opening_hours()

    def process(self):
        return self.__good, self.__bad

    def __is_in_water(self):
        data = self.__db.query_poi_in_water(self.__lon, self.__lat)
        if data.empty:
            self.__good.append('not_in_water')
        else:
            self.__bad.append('in_water')

    def __custom_opening_hours(self):
        if self.__opening_hours is not None:
            if 'comment' in self.__opening_hours or 'status' in self.__opening_hours or 'dawn' in self.__opening_hours or 'sunrise' in self.__opening_hours or 'sunset' in self.__opening_hours or 'dusk' in self.__opening_hours:
                self.__bad.append('custom_opening_hours')
            else:
                self.__good.append('standard_opening_hours')

    def __is_name_road_around(self):
        data = self.__db.query_name_road_around(self.__lon, self.__lat, self.__street, True, 'name')
        if data.empty:
            self.__bad.append('street_is_not_around')
        else:
            self.__good.append('street_is_around')


    def __is_name_metaphone_road_around(self):
        data = self.__db.query_name_road_around(self.__lon, self.__lat, self.__street, True, 'metaphone')
        if data.empty:
            self.__bad.append('street_metaphone_is_not_around')
        else:
            self.__good.append('street_metaphone_is_around')