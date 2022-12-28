# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_string, clean_street
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_osm_city_name
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_deichmann(DataProvider):

    def contains(self):
        self.link = 'https://www.deichmann.com/hu-hu/rest/v2/deichmann-hu/mosaic/stores?latitude=47.6874569&longitude=17.6503974&pageSize=10000&radius=1000000&fields=FULL&format=json&lang=hu_HU'
        self.tags = {'shop': 'shoes', 'operator': ' Deichmann Cipőkereskedelmi Kft.',
                     'operator:addr': '1134 Budapest, Kassák Lajos utca 19-25. 6. emelet', 'ref:vatin:hu': '12583083-2-44',
                     'ref:vatin': 'HU12583083', 'ref:HU:company': '01 09 693582', 'brand': 'CCC', 'brand:wikidata': 'Q11788344',
                     'brand:wikipedia': 'hu:Deichmann', 'contact:email': 'ugyfelszolgalat@deichmann.com',
                     'phone': '+36 80 840 840',
                     'contact:facebook': 'https://www.facebook.com/Deichmann.HU/',
                     'contact:youtube': 'https://www.youtube.com/user/Deichmann/',
                     'contact:instagram': 'https://www.instagram.com/deichmannhu/',
                     'contact:tiktok': 'https://www.tiktok.com/@deichmann',
                     'contact:pinterest': 'https://www.pinterest.de/deichmannde/',
                     'contact:linkedin': 'https://de.linkedin.com/company/deichmann',
                     'air_conditioning': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hudeichsho = self.tags.copy()
        hudeichsho.update(POS_HU_GEN)
        hudeichsho.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hudeichsho', 'poi_common_name': 'Deichmann', 'poi_type': 'shoes',
             'poi_tags': hudeichsho, 'poi_url_base': 'https://www.deichmann.com/hu-hu', 'poi_search_name': 'deichmann',
             'poi_search_avoid_name': '(ccc)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 50},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text.get('stores'):
                    try:
                        self.data.code = 'hudeichsho'
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('geoPoint').get('latitude'),
                                                                         poi_data.get('geoPoint').get('longitude'))
                        self.data.postcode = clean_string(poi_data.get('address').get('postalCode'))
                        self.data.city = clean_city(poi_data.get('address').get('town'))
                        self.data.street = clean_street(poi_data.get('address').get('line1'))
                        self.data.housenumber = clean_string(poi_data.get('address').get('line2'))
                        self.data.phone = clean_string(poi_data.get('address').get('phone'))
                        self.data.original = clean_string(poi_data.get('street'))
                        try:
                            for i in range(7):
                                opening = poi_data.get('openingHours').get('weekDayOpeningList')[i].get('openingTime').get('formattedHour')
                                closing = poi_data.get('openingHours').get('weekDayOpeningList')[i].get('closingTime').get('formattedHour')
                                self.data.day_open(i, opening)
                                self.data.day_close(i, closing)
                        except AttributeError:
                            logging.info('Non existing opening hours value: {}'.format(poi_data))
                        except Exception as e:
                            logging.exception('Exception occurred: {}'.format(e))
                            logging.exception(traceback.print_exc())

                        self.data.branch = clean_string(poi_data.get('address').get('appartment'))
                        self.data.public_holiday_open = False
                        self.data.ref = clean_string(poi_data.get('name'))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
