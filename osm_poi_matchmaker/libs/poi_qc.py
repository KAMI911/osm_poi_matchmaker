# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sqlalchemy
    from sqlalchemy.orm import scoped_session, sessionmaker
    import geopandas as gpd
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

class POIQC:

    def __init__(self, db, lon, lat):
        self.__db = db
        self.__lon = lon
        self.__lat = lat
        self.__good = []
        self.__bad = []
        self.__distance = 1
        self.__check()

    def __check(self):
        self.__is_in_water()


    def  process(self):
        return self.__good, self.__bad


    def __is_in_water(self):
        data = self.__db.query_poi_in_water(self.__lon, self.__lat)
        if data.empty:
            self.__good.append('not_in_water')
        else:
            self.__bad.append('in_water')
