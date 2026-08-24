# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    import requests
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_string, clean_url
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# The widget moved from shellgsllocator to shellretaillocator and its API from v1 to v2. A plain
# within_bounds query over all of Hungary returns clusters instead of individual stations once an
# area has "too many" of them (Budapest alone), so the whole country has to be queried recursively:
# split the bounding box into quadrants wherever the response is clustered, and keep the quadrant's
# individual locations otherwise. The per-station field names are unchanged from the old v1 feed.
BASE_URL = 'https://shellretaillocator.geoapp.me/api/v2/locations/within_bounds'
HU_BBOX = ((45.7, 16.0), (48.6, 22.9))
MAX_SPLIT_DEPTH = 6


class hu_shell(DataProvider):

    def contains(self):
        self.link = BASE_URL
        self.tags = {'amenity': 'fuel', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}
        self.tags.update(POS_HU_GEN)
        self.tags.update({'loyalty_card': 'yes'})
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hushellfu = self.tags.copy()
        hushellfu.update({'brand': 'Shell', 'contact:phone': '+36 1 480 1114',
                          'contact:fax': '+36 1 999 8673', 'contact:website': 'https://shell.hu/',
                          'contact:facebook': 'ShellMagyarorszag', 'contact:twitter': 'shell',
                          'brand:wikidata': 'Q154950', 'brand:wikipedia': 'hu:Royal Dutch Shell',
                          'air_conditioning': 'yes'})
        self.__types = [
            {'poi_code': 'hushellfu', 'poi_common_name': 'Shell', 'poi_type': 'fuel', 'poi_tags': hushellfu,
             'poi_url_base': 'https://shell.hu', 'poi_search_name': 'shell',
             'poi_search_avoid_name': '(mol|m. petrol|avia|lukoil|hunoil)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 60},
        ]
        return self.__types

    def __fetch_locations(self, sw=None, ne=None, depth=0, results=None):
        """Recursively fetch every Hungarian station within a bounding box, splitting into
        quadrants wherever the API still returns clusters instead of individual locations."""
        if results is None:
            results = {}
        if sw is None:
            sw, ne = HU_BBOX
        url = '{}?sw[]={}&sw[]={}&ne[]={}&ne[]={}&format=json'.format(BASE_URL, sw[0], sw[1], ne[0], ne[1])
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        for loc in data.get('locations') or []:
            if loc.get('country_code') == 'HU':
                results[loc['id']] = loc
        if data.get('clusters') and depth < MAX_SPLIT_DEPTH:
            mid_lat = (sw[0] + ne[0]) / 2
            mid_lng = (sw[1] + ne[1]) / 2
            for qsw, qne in [((sw[0], sw[1]), (mid_lat, mid_lng)),
                             ((sw[0], mid_lng), (mid_lat, ne[1])),
                             ((mid_lat, sw[1]), (ne[0], mid_lng)),
                             ((mid_lat, mid_lng), (ne[0], ne[1]))]:
                self.__fetch_locations(qsw, qne, depth + 1, results)
        return results

    def process(self):

        try:
            cache_file = os.path.join(self.download_cache, self.filename)
            if config.get_download_use_cached_data() is True and os.path.isfile(cache_file):
                with open(cache_file, mode='r', encoding='utf-8') as file:
                    stations = json.load(file)
            else:
                stations = list(self.__fetch_locations().values())
                if not os.path.exists(self.download_cache):
                    os.makedirs(self.download_cache)
                with open(cache_file, mode='w', encoding='utf-8') as file:
                    json.dump(stations, file)
            if stations is not None:
                for poi_data in stations:
                    try:
                        if poi_data.get('country_code') == 'HU':
                            logging.debug('Shell fuel station in Hungary')
                        else:
                            logging.info('Shell fuel station NOT in Hungary')
                            continue
                        self.data.code = 'hushellfu'
                        self.data.website = clean_url(poi_data.get('website_url')) if ('website_url' in poi_data and poi_data.get('website_url') != '') else 'https://shell.hu/'
                        self.data.postcode = clean_string(poi_data.get('postcode')) if ('postcode' in poi_data and poi_data.get('postcode') != '') else None
                        street_tmp = poi_data.get('address').lower().split()
                        for i in range(0, len(street_tmp) - 2):
                            street_tmp[i] = street_tmp[i].capitalize()
                        street_tmp = ' '.join(street_tmp)
                        if 'city' in poi_data and poi_data.get('city') != '':
                            self.data.city = clean_city(poi_data.get('city').title())
                        else:
                            if 'name' in poi_data and poi_data.get('name') != '':
                                self.data.city = clean_city(
                                    poi_data.get('name').title())
                            else:
                                self.data.city = None
                        if 'name' in poi_data and poi_data.get('name') != '':
                            self.data.branch = poi_data.get('name').strip()
                        if 'twenty_four_hour' in poi_data.get('amenities'):
                            self.data.nonstop = True
                            self.data.public_holiday_open = True
                        self.data.original = poi_data.get('address') if ('address' in poi_data and poi_data.get('address') != '') else None
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'),
                                                                        poi_data.get('lng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            street_tmp)
                        self.data.phone = clean_phone_to_str(str(poi_data.get('telephone'))) if ('telephone' in poi_data and poi_data.get('telephone') != '') else None
                        self.data.email = None
                        self.data.fuel_octane_95 = True
                        self.data.fuel_diesel = True
                        self.data.fuel_octane_100 = True
                        self.data.fuel_diesel_gtl = True
                        if 'air_and_water' in poi_data.get('amenities'):
                            self.data.compressed_air = True
                        # TODO: Separete adblue_pack, adblue_car and adblue_truck
                        if 'adblue_pack' in poi_data.get('amenities') or 'adblue_car' in poi_data.get('amenities') or 'adblue_truck' in poi_data.get('amenities'):
                            self.data.fuel_adblue = True
                        if 'hot_food' in poi_data.get('amenities'):
                            self.data.restaurant = True
                        if 'bakery_shop' in poi_data.get('amenities') or 'food_offerings' in poi_data.get('amenities'):
                            self.data.food = True
                        if 'hgv_lane' in poi_data.get('amenities'):
                            self.data.truck = True
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
