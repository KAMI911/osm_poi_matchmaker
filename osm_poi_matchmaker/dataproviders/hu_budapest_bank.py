# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import json
    import os
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_budapest_bank(DataProvider):


    def constains(self):
        self.link = 'https://www.budapestbank.hu/info/fiokkereso/process/get_data.php?action=get_data_json'
        self.POI_COMMON_TAGS = "'brand': 'Budapest Bank', 'brand:wikidata': 'Q27493463', " \
                               "'brand:wikipedia': 'en:Budapest Bank', 'operator': 'Budapest Bank Zrt.', "
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hubpbank', 'poi_name': 'Budapest Bank', 'poi_type': 'bank',
                 'poi_tags': "{'amenity': 'bank', 'bic': 'BUDAHUHB', 'atm': 'yes', " + self.POI_COMMON_TAGS +
                 " 'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.budapestbank.hu', 'poi_search_name': '(budapest bank|bp bank)',
                 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 10},
                {'poi_code': 'hubpatm', 'poi_name': 'Budapest Bank ATM', 'poi_type': 'atm',
                 'poi_tags': "{'amenity': 'atm', " + self.POI_COMMON_TAGS + " }",
                 'poi_url_base': 'https://www.budapestbank.hu', 'poi_search_name': '(budapest bank|bp bank)',
                 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 80, 'osm_search_distance_unsafe': 10}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                text = json.loads(soup.get_text())
                for poi_data in text['points']:
                    if poi_data['fiok'] == 1:
                        self.data.name = 'Budapest Bank'
                        self.data.code = 'hubpbank'
                        self.data.public_holiday_open = False
                    else:
                        self.data.name = 'Budapest Bank ATM'
                        self.data.code = 'hubpatm'
                        self.data.public_holiday_open = True
                    self.data.postcode = poi_data['zip']
                    self.data.city = poi_data['city_only']
                    if self.data.code == 'hubpatm':
                        self.data.nonstop = True
                    else:
                        self.data.nonstop = False
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['latitude'], poi_data['longitude'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(poi_data['addr'])
                    self.data.original = poi_data['address']
                    self.data.branch = poi_data['name']
                    # Processing opening hours
                    oh = []
                    if poi_data.get('opening') is not None:
                        opening = poi_data.get('opening').split('||')
                        for i in opening:
                            if 'H:' in opening[i]:
                                self.data.day_open(0,  opening[i].replace('H:','').split('-')[0].strip())
                                self.data.day_close(0, opening[i].replace('H:','').split('-')[1].strip())
                            elif 'K:' in opening[i]:
                                self.data.day_open(1, opening[i].replace('K:', '').split('-')[0].strip())
                                self.data.day_close(1, opening[i].replace('K:', '').split('-')[1].strip())
                            elif 'Sz:' in opening[i]:
                                self.data.day_open(2, opening[i].replace('Sz:', '').split('-')[0].strip())
                                self.data.day_close(2, opening[i].replace('Sz:', '').split('-')[1].strip())
                            elif 'Cs:' in opening[i]:
                                self.data.day_open(3, opening[i].replace('Cs:', '').split('-')[0].strip())
                                self.data.day_close(3, opening[i].replace('Cs:', '').split('-')[1].strip())
                            elif 'P:' in opening[i]:
                                self.data.day_open(4, opening[i].replace('P:', '').split('-')[0].strip())
                                self.data.day_close(4, opening[i].replace('P:', '').split('-')[1].strip())
                    self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
