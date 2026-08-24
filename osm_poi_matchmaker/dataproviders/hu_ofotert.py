# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    import requests
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.utils import config
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

# The store search POST needs a CSRF token that only exists as a <meta name="main.csrf"> tag
# rendered into the search page itself (not a cookie, not embedded anywhere in the JS bundle), so
# the fetch has to be a real two-step session: load the page, then POST with that token. A single
# query returns every store nationwide regardless of the lat/lng given - confirmed by querying from
# three different corners of the country and getting back the identical set of station ids - so no
# pagination or bounding-box tiling is needed here.
SEARCH_PAGE_URL = 'https://www.ofotert.hu/hu/uzletkereso'
API_URL = 'https://www.ofotert.hu/hu/api/store-locator'


class hu_ofotert(DataProvider):


    def contains(self):
        self.link = API_URL
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


    def __fetch_stores(self):
        session = requests.Session()
        page = session.get(SEARCH_PAGE_URL, timeout=60)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        csrf_tag = soup.find('meta', {'name': 'main.csrf'})
        if csrf_tag is None or not csrf_tag.get('content'):
            logging.warning('Could not find CSRF token on the store locator page.')
            return []
        data = {'lat': '47.1625', 'lng': '19.5033', 'template': '/hu/template/store-locator',
                'default': '', 'csrf': csrf_tag.get('content')}
        response = session.post(API_URL, data=data, timeout=60)
        response.raise_for_status()
        return response.json().get('stores') or []

    def process(self):
        try:
            cache_file = os.path.join(self.download_cache, self.filename)
            if config.get_download_use_cached_data() is True and os.path.isfile(cache_file):
                with open(cache_file, mode='r', encoding='utf-8') as file:
                    stores = json.load(file)
            else:
                stores = self.__fetch_stores()
                if not os.path.exists(self.download_cache):
                    os.makedirs(self.download_cache)
                with open(cache_file, mode='w', encoding='utf-8') as file:
                    json.dump(stores, file)
            for poi_data in stores or []:
                try:
                    self.data.code = 'huofoteopt'
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('geo_lat'), poi_data.get('geo_lng'))
                    self.data.postcode = clean_string(poi_data.get('address_zip'))
                    self.data.branch = clean_string(poi_data.get('name'))
                    self.data.city = clean_city(poi_data.get('address_city'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data.get('address_street'))
                    self.data.phone = clean_phone_to_str(';'.join(poi_data.get('phone_numbers') or []))
                    self.data.original = clean_string(poi_data.get('address_street'))
                    try:
                        # opening_times is Sunday-first (index 0 = Sunday, 1 = Monday, ... 6 =
                        # Saturday); our day index is Monday-first (0 = Monday ... 6 = Sunday).
                        opening_times = poi_data.get('opening_times') or []
                        for i, raw_hours in enumerate(opening_times):
                            cleaned = clean_opening_hours(raw_hours)
                            if cleaned is not None and cleaned != "":
                                opening, closing = cleaned
                                self.data.day_open((i - 1) % 7, opening)
                                self.data.day_close((i - 1) % 7, closing)
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                    self.data.public_holiday_open = False
                    self.data.ref = clean_string(poi_data.get('gv_id'))
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
