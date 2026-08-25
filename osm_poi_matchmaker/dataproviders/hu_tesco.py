# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import time
    import random
    import traceback
    import requests
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_OTP, PAY_CASH
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# The old tesco.hu/Ajax bounds-search endpoint is gone (site rebuilt); the current store locator
# (a Yext-powered widget at /aruhazak/) exposes this instead. It needs the X-Requested-With header
# below or it silently returns an empty 200 body. Unlike the old endpoint it isn't bounds-based -
# it always returns the nearest stores to the given point within a fixed ~25 km radius that no
# query parameter (radius=/distance=/searchRadius= all tried, none had any effect) can widen. So
# covering the whole country means querying a grid of points spaced closely enough that every
# store falls within 25 km of at least one of them, then deduplicating by the stable c_bRANCH_NO2
# branch id (confirmed present on every entity; distinct from the old feed's "goldid").
SEARCH_URL = 'https://www.tesco.hu/aruhazak/searchapi'
HU_BBOX = ((45.7, 16.0), (48.6, 22.9))  # (lat, lon) sw, ne - same country box as hu_shell.py
GRID_STEP_LAT = 0.3   # ~33 km
GRID_STEP_LON = 0.45  # ~33 km at this latitude
# Be polite (and avoid tripping Akamai bot detection with a request burst): pause between
# grid-point requests instead of hammering the endpoint 160 times back-to-back. Jittered so the
# pacing doesn't look like an obvious bot pattern either.
REQUEST_DELAY_MIN_SECONDS = 0.4
REQUEST_DELAY_MAX_SECONDS = 1.1
REQUEST_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0 Safari/537.36',
}
STORE_TYPE_TO_CODE = {'Express': 'hutescoexp', 'Hypermarket': 'hutescoext', 'Supermarket': 'hutescosup'}
DAY_ORDER = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']


class hu_tesco(DataProvider):

    def contains(self):
        self.link = SEARCH_URL
        self.tags = {'operator': 'TESCO-GLOBAL Áruházak Zrt.',
                     'operator:addr': '2040 Budaörs, Kinizsi út 1-3.',
                     'ref:HU:company': '13-10-040628', 'ref:HU:vatin': '10307078-2-44',
                     'ref:vatin': 'HU10307078', 'brand': 'Tesco',
                     'brand:wikipedia': 'hu:Tesco',
                     'internet_access': 'wlan', 'internet_access:fee': 'no',
                     'internet_access:ssid': 'tesco-internet',
                     'contact:facebook': 'tescoaruhazak',
                     'contact:pinterest': 'tescohungary',
                     'contact:youtube': 'https://www.youtube.com/user/TescoMagyarorszag',
                     'loyalty_card': 'yes', 'payment:gift_card': 'yes', 'payment:wire_transfer': 'yes',
                     'air_conditioning': 'yes'}
        self.tags.update(POS_OTP)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hutescoexp = {'shop': 'convenience', 'brand:wikidata': 'Q98456772', 'additional_ref_name': 'ref'}
        hutescoexp.update(self.tags)
        hutescoext = {'shop': 'supermarket',
                      'wheelchair': 'yes', 'source:wheelchair': 'website', 'brand:wikidata': 'Q25172225'}
        hutescoext.update(self.tags)
        hutescosup = {'shop': 'supermarket',
                      'wheelchair': 'yes', 'source:wheelchair': 'website', 'brand:wikidata': 'Q487494'}
        hutescosup.update(self.tags)
        husmrktexp = {'shop': 'convenience', 'alt_name': 'Tesco Expressz', 'brand:wikidata': 'Q487494'} # TODO: create wikidata tag
        husmrktexp.update(self.tags)
        husmrktsup = {'shop': 'supermarket', 'wheelchair': 'yes',
                      'source:wheelchair': 'website', 'alt_name': 'Tesco', 'brand:wikidata': 'Q487494'} # TODO: create wikidata tag
        husmrktsup.update(self.tags)
        self.__types = [
            {'poi_code': 'hutescoexp', 'poi_common_name': 'Tesco Expressz', 'poi_type': 'shop',
             'poi_tags': hutescoexp, 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
            {'poi_code': 'hutescoext', 'poi_common_name': 'Tesco Extra', 'poi_type': 'shop',
             'poi_tags': hutescoext, 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100},
            {'poi_code': 'hutescosup', 'poi_common_name': 'Tesco', 'poi_type': 'shop',
             'poi_tags': hutescosup, 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100},
            {'poi_code': 'husmrktexp', 'poi_common_name': 'S-Market', 'poi_type': 'shop',
             'poi_tags': husmrktexp, 'poi_url_base': 'https://tesco.hu',
             'poi_search_name': '(tesco|smarket|s-market|s market)',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
            {'poi_code': 'husmrktsup', 'poi_common_name': 'S-Market', 'poi_type': 'shop',
             'poi_tags': husmrktsup, 'poi_url_base': 'https://tesco.hu',
             'poi_search_name': '(tesco|smarket|s-market|s market)',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
        ]
        return self.__types

    @staticmethod
    def __grid_points():
        (sw_lat, sw_lon), (ne_lat, ne_lon) = HU_BBOX
        lat = sw_lat
        while lat <= ne_lat:
            lon = sw_lon
            while lon <= ne_lon:
                yield round(lat, 4), round(lon, 4)
                lon += GRID_STEP_LON
            lat += GRID_STEP_LAT

    def __fetch_stores(self):
        """Query a grid of points covering Hungary and merge the results, deduplicated by
        the stable c_bRANCH_NO2 branch id (a single query only returns stores within a fixed
        ~25 km radius of the given point, see module docstring)."""
        stores = {}
        session = requests.Session()
        session.headers.update(REQUEST_HEADERS)
        grid = list(self.__grid_points())
        for i, (lat, lon) in enumerate(grid):
            try:
                response = session.get(SEARCH_URL, params={'q': '{},{}'.format(lat, lon), 'l': 'hu'},
                                       timeout=30)
                response.raise_for_status()
                data = response.json()
                for entity in (data.get('response') or {}).get('entities') or []:
                    profile = entity.get('profile') or {}
                    branch_id = profile.get('c_bRANCH_NO2')
                    if branch_id and branch_id not in stores:
                        stores[branch_id] = profile
            except Exception as e:
                logging.exception('Exception occurred while fetching Tesco grid point %s,%s: %s', lat, lon, e)
                logging.exception(traceback.format_exc())
            if i < len(grid) - 1:
                time.sleep(random.uniform(REQUEST_DELAY_MIN_SECONDS, REQUEST_DELAY_MAX_SECONDS))
        return list(stores.values())

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
            for profile in stores:
                try:
                    name = profile.get('name') or ''
                    address = profile.get('address') or {}
                    city = clean_city(address.get('city'))
                    store_types = profile.get('c_sTORE_TYPE') or []
                    store_type = store_types[0] if store_types else None

                    if store_type == 'Express':
                        if city not in ['Győr', 'Sopron', 'Mosonmagyaróvár', 'Levél']:
                            self.data.code = 'hutescoexp'
                        else:
                            self.data.code = 'husmrktexp'
                    elif store_type == 'Hypermarket':
                        self.data.code = 'hutescoext'
                    else:
                        if city not in ['Levél']:
                            self.data.code = 'hutescosup'
                        else:
                            self.data.code = 'husmrktsup'

                    self.data.branch = clean_string(name)
                    self.data.ref = clean_string(profile.get('c_bRANCH_NO2'))
                    website = profile.get('c_mainStorePageURL') or profile.get('websiteUrl')
                    if website:
                        self.data.website = website

                    coord = profile.get('geocodedCoordinate') or {}
                    self.data.lat, self.data.lon = check_hu_boundary(coord.get('lat'), coord.get('long'))

                    line1 = address.get('line1')
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(line1)
                    self.data.postcode = clean_string(address.get('postalCode'))
                    self.data.city = city
                    self.data.original = line1

                    phone = (profile.get('mainPhone') or {}).get('number')
                    if phone:
                        self.data.phone = clean_phone_to_str(phone)

                    def hhmm(value):
                        return '{:02d}:{:02d}'.format(value // 100, value % 100)

                    normal_hours = {h.get('day'): h for h in (profile.get('hours') or {}).get('normalHours') or []}
                    for i, day_name in enumerate(DAY_ORDER):
                        day = normal_hours.get(day_name)
                        if day and not day.get('isClosed') and day.get('intervals'):
                            first = day['intervals'][0]
                            self.data.day_open(i, hhmm(first['start']))
                            self.data.day_close(i, hhmm(first['end']))
                        else:
                            self.data.day_open_close(i, None, None)

                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(profile)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
