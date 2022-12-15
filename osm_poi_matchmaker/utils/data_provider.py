# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import clean_city, \
        clean_javascript_variable, clean_opening_hours_2, clean_phone
    from osm_poi_matchmaker.libs.poi_dataset import POIDatasetRaw
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POI_DATA = ''


class DataProvider:

    def __init__(self, session, download_cache, filetype=FileType.json, verify_link=True):
        self.session = session
        self.download_cache = download_cache
        self.filename = '{}.{}'.format(self.__class__.__name__, filetype)
        self.filetype = None
        self.verify_link = verify_link
        self.link = None
        self.POI_COMMON_TAGS = None
        self.headers = None
        self.post = None
        self.tags = None
        self.__types = None
        self.contains()
        self.data = POIDatasetRaw()

    def contains(self):
        self.POI_COMMON_TAGS = ""
        self.link = ''

    def types(self):
        self.__types = []
        return self.__types

    def process(self):
        pass

    def export_list(self):
        if self.data is None or self.data.lenght() < 1:
            logging.warning('Resultset is empty. Skipping ...')
        else:
            insert_poi_dataframe(self.session, self.data.process())
