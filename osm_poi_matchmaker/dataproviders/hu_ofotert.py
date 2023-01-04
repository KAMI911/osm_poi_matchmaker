# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_string, clean_street, clean_opening_hours
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_osm_city_name
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_ofotert(DataProvider):


    def contains(self):
        self.link = 'https://www.ofotert.hu/hu/api/store-locator'
        self.tags = {'shop': 'optician', 'operator': 'GrandVision Hungary Kft.',
                     'operator:addr': '1113 Budapest, Bocskai út 134-146.', 'ref:HU:vatin': '12142143-2-44',
                     'ref:vatin': 'HU12142143', 'ref:HU:company': '01-09-468765', 'brand': 'Ofotért',
                     'contact:email': 'vevoszolgalat@ofotert.hu',
                     'contact:facebook': 'https://www.facebook.com/ofotert',
                     'contact:youtube': 'https://www.youtube.com/user/ofoterthu',
                     'air_conditioning': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)


    def types(self):
        huofoteopt = self.tags.copy()
        huofoteopt.update(POS_HU_GEN)
        huofoteopt.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'ofoteopt', 'poi_common_name': 'Ofotért', 'poi_type': 'optician',
             'poi_tags': huofoteopt, 'poi_url_base': 'https://www.ofotert.hu', 'poi_search_name': 'ofotért',
             'poi_search_avoid_name': '(vision)',
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
                        self.data.code = 'huofoteopt'
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('geo_lat'), poi_data.get('geo_lng'))
                        self.data.postcode = clean_string(poi_data.get('address_zip'))
                        self.data.branch = clean_string(poi_data.get('name'))
                        self.data.city = clean_city(poi_data.get('address_city'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            poi_data.get('address_street'))
                        self.data.phone = clean_string(poi_data.get('phone_numbers'))
                        self.data.original = clean_string(poi_data.get('address_street'))
                        try:
                            for i in range(7):
                                if clean_opening_hours(poi_data.get('opening_times')[i]) is not None and \
                                   clean_opening_hours(poi_data.get('opening_times')[i]) != "":
                                    opening, closing = clean_opening_hours(poi_data.get('opening_times')[i])
                                    self.data.day_open(i, opening)
                                    self.data.day_close(i, closing)
                        except Exception as e:
                            logging.exception('Exception occurred: {}'.format(e))
                            logging.exception(traceback.print_exc())
                        self.data.public_holiday_open = False
                        self.data.ref = clean_string(poi_data.get('gv_id'))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
