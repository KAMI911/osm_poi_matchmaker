# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    import os
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception("Exception occurred")
    sys.exit(128)


class hu_sber_bank(DataProvider):


    def constains(self):
        self.link = 'https://www.sberbank.hu/servlet/maplocatorServlet'
        self.POI_COMMON_TAGS = "'brand': 'Sberbank', 'brand:wikidata': 'Q17379757', " \
                               "'brand:wikipedia': 'en:Sberbank of Russia', 'operator': 'Sberbank Magyarország Zrt.', "\
                               "'operator:addr': '1088 Budapest, Rákóczi út 1-3.', 'brand:ru': 'Сбербанк', " \
                               "'name:ru': 'Сбербанк', 'ref:vatin': 'HU10776999', 'ref:vatin:hu': '10776999-2-44', " \
                               "'ref:HU:company': '01 10 041720', "
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [{'poi_code': 'husberbank', 'poi_name': 'Sberbank', 'poi_type': 'bank',
                 'poi_tags': "{'amenity': 'bank', 'bic': 'MAVOHUHB', 'atm': 'yes', " + self.POI_COMMON_TAGS +
                 " 'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.sberbank.hu', 'poi_search_name': '(sber|sberbank)',
                 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 10},
                {'poi_code': 'husberatm', 'poi_name': 'Sberbank ATM', 'poi_type': 'atm',
                 'poi_tags': "{'amenity': 'atm', " + self.POI_COMMON_TAGS + " }",
                 'poi_url_base': 'https://www.sberbank.hu', 'poi_search_name': '(sber|sberbank)',
                 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 80, 'osm_search_distance_unsafe': 10}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text['atmList']:
                    self.data.name = 'Sberbank ATM'
                    self.data.code = 'husberatm'
                    self.data.public_holiday_open = True if poi_data.get('atmNonstop') is True else False
                    self.data.postcode = poi_data.get('address')['zipCode']
                    ctmp = poi_data.get('address')['city']
                    self.data.city = ctmp if 'kerület' not in ctmp else poi_data.get('address')['county']
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('address')['coordinateX'],
                                                                     poi_data.get('address')['coordinateY'])
                    street_tmp = '{} {}'.format(poi_data.get('address')['street'],
                                                poi_data.get('address')['houseNumber'].split('.')[0])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(street_tmp)
                    self.data.original = street_tmp
                    self.data.add()
                for poi_data in text['branchList']:
                    self.data.name = 'Sberbank'
                    self.data.code = 'husberbank'
                    self.data.public_holiday_open = False
                    self.data.postcode = poi_data.get('address')['zipCode']
                    ctmp = poi_data.get('address')['city']
                    self.data.city = ctmp if 'kerület' not in ctmp else poi_data.get('address')['county']
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('address')['coordinateX'],
                                                                     poi_data.get('address')['coordinateY'])
                    street_tmp = '{} {}'.format(poi_data.get('address')['street'],
                                                poi_data.get('address')['houseNumber'].split('.')[0])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(street_tmp)
                    self.data.original = street_tmp
                    self.data.email = poi_data.get('emailAppointment')
                    self.data.phone = poi_data.get('phone'.split('/')[0])
                    for i, opening in enumerate(poi_data.get('openTime')):
                        if opening is not None and opening != '':
                            try:
                                oh = opening.get('from') if opening.get('from') != '' else None
                                self.data.day_open(i, oh)
                                ch = opening.get('to') if opening.get('to') != '' else None
                                self.data.day_close(i, ch)
                            except Exception as e:
                                logging.info(opening)
                                logging.exception("Exception occurred")
                                logging.error(e)
                                continue
                        else:
                            logging.debug('There is no opening hours on day: {}.'.format(i))
                    self.data.add()
        except Exception as e:
            logging.exception("Exception occurred")
            logging.error(e)
