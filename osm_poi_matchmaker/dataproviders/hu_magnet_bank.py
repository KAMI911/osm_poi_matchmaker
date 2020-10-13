# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    import os
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_magnet_bank(DataProvider):


    def constains(self):
        #self.link = 'https://www.magnetbank.hu/kapcsolat/fiokkereso'
        self.link = os.path.join(config.get_directory_cache_url(), 'hu_magnet_bank.json')
        self.POI_COMMON_TAGS = "'brand': 'MagNet Bank', 'brand:wikidata': 'Q17379757', " \
                               "'brand:wikipedia': 'hu:MagNet Bank', 'operator': 'MagNet Magyar Közösségi Bank Zrt.', "\
                               "'operator:addr': '1062 Budapest, Andrássy út 98.', 'contact:fax': '+36 1 428 8889', " \
                               "'ref:HU:company': '01 10 046111', 'ref:vatin': 'HU14413591' ," \
                               "'ref:vatin:hu': '14413591-4-44', "
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [
            {'poi_code': 'humagnbank', 'poi_name': 'MagNet Bank', 'poi_type': 'bank',
             'poi_tags': "{'amenity': 'bank', 'bic': 'HBWEHUHB', 'atm': 'yes', " + self.POI_COMMON_TAGS +
             " 'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.magnetbank.hu', 'poi_search_name': '(magnet bank|magnetbank)',
                 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
                 'osm_search_distance_unsafe': 10},
                {'poi_code': 'humagnatm', 'poi_name': 'MagNet Bank ATM', 'poi_type': 'atm',
                 'poi_tags': "{'amenity': 'atm', " + self.POI_COMMON_TAGS + " }",
                 'poi_url_base': 'https://www.magnetbank.hu',
                 'poi_search_name': '(magnet bank|magnetbank|magnet bank atm|magnet atm)',
                 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 80, 'osm_search_distance_unsafe': 10}
                ]
        return self.__types

    def process(self):
        try:
            if self.link:
                with open(self.link, 'r') as f:
                    text = json.load(f)
                    for poi_data in text['result']:
                        if poi_data.get('address') is not None:
                            if poi_data.get('type') == '1':
                                self.data.name = 'MagNet Bank ATM'
                                self.data.code = 'humagnatm'
                                self.data.public_holiday_open = True
                            elif poi_data.get('type') in ['0', '2']:
                                self.data.name = 'MagNet Bank'
                                self.data.code = 'humagnbank'
                                self.data.public_holiday_open = False
                                self.data.email = poi_data.get('email')
                                self.data.phone = '+36 1 428 8888'
                            else:
                                logging.info('Unknow type! ({})'.format(poi_data.get('type')))
                            self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                            self.data.conscriptionnumber = extract_all_address(poi_data.get('address'))
                            self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lon'))
                            self.data.original = poi_data.get('address')
                        self.data.add()
        except Exception as e:
            logging.exception('Exception occurred')

            logging.error(e)
