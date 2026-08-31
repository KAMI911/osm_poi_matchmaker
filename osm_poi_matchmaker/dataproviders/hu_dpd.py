# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import math
    import time
    import random
    import traceback
    from curl_cffi import requests
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.address import clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# DPD's own site (dpdgroup.com/hu/mydpd/parcel-shops) renders a JS/Woosmap-driven map whose
# visible search box only ever updates a fixed default (Budapest) result set when queried
# with plain HTTP - the actual per-location search is a JSON POST to this sibling endpoint,
# found by intercepting the page's own fetch() calls in a real browser. It needs a Spring
# Security CSRF token (a plain <meta name="_csrf"> tag on any page of the site, not a bot
# challenge) and session cookies from a prior GET - both obtained here with an ordinary
# request, no browser automation needed. Akamai/Cloudflare still blocks plain requests/
# urllib3 by TLS fingerprint (confirmed 403 on both the GET and the POST), so this uses
# curl_cffi with Firefox impersonation, same as hu_tesco.py.
PARCEL_SHOPS_PAGE_URL = 'https://www.dpdgroup.com/hu/mydpd/parcel-shops'
PARCEL_SHOP_MAP_URL = 'https://www.dpdgroup.com/hu/mydpd/parcel-shop-map'
IMPERSONATE = 'firefox135'
REQUEST_HEADERS = {
    'Content-Type': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
}
# Be polite and avoid an obvious burst-of-identical-requests pattern.
REQUEST_DELAY_MIN_SECONDS = 0.4
REQUEST_DELAY_MAX_SECONDS = 1.1

HU_BBOX = ((45.7, 16.0), (48.6, 22.9))  # (lat, lon) sw, ne - same country box as hu_shell.py/hu_tesco.py
# The parcel-shop-map endpoint always returns exactly 10 shops - the nearest ones to
# the given point, no matter how sparse or dense the area (confirmed: a rural point still
# gets 10 results, just from up to ~27km away) - so "got a full page" can't signal "there
# might be more nearby" the way it does for hu_tesco.py's endpoint. What it does say is
# whether this cell is dense enough to be worth a closer look: if every one of the 10
# nearest shops already lies within this cell's own radius, they're clustered near the
# center and an (unreturned) 11th, 12th, ... might be just outside that radius - so the
# cell is split into four quadrants (a simple quadtree) up to MAX_SUBDIVISION_DEPTH. If
# instead the search had to reach out to (or past) the cell's radius to fill 10 results,
# the area is sparse enough that a neighbouring base grid point's own search will pick up
# whatever's further out - no local subdivision needed. This keeps the extra requests
# concentrated on actually-dense areas (city centres) instead of blowing up the total
# request count nationwide.
BASE_GRID_STEP_LAT = 0.25
BASE_GRID_STEP_LON = 0.35
MAX_SUBDIVISION_DEPTH = 3
DAY_ORDER = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']
# 400 = self-service DPD Box/GLSBox/AlzaBox locker; every other pudoType seen (100, 200)
# is a staffed retail partner hosting a Pickup point (confirmed by cross-checking real
# entries, e.g. a wine shop and a shoe-repair kiosk both come back as 100/200).
LOCKER_PUDO_TYPE = 400


class hu_dpd(DataProvider):
    """Imports DPD Pickup parcel shop and DPD Box/GLSBox/AlzaBox parcel locker locations in
    Hungary from DPD's mydpd parcel-shop-map JSON search API."""

    def contains(self):
        self.link = PARCEL_SHOP_MAP_URL
        self.tags = {'operator': 'DPD Hungary Kft.',
                     'operator:addr': '1134 Budapest, Váci út 33., A épület II. emelet',
                     'ref:HU:company': '01-09-888141', 'ref:HU:vatin': '13034283-2-44',
                     'ref:vatin': 'HU13034283', 'brand': 'DPD', 'brand:wikidata': 'Q541030',
                     'brand:wikipedia': 'en:Geopost'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hudpdcso = {'amenity': 'parcel_locker', 'parcel_mail_in': 'yes', 'parcel_pickup': 'yes'}
        hudpdcso.update(self.tags)
        hudpdpp = {'post_office': 'post_partner', 'post_office:brand': 'DPD Pickup',
                  'post_office:brand:wikidata': 'Q541030', 'post_office:parcel_pickup': 'yes'}
        hudpdpp.update(self.tags)
        avoid_name = '(gls|foxpost|alzabox|alza|packeta|z-box|postapont|easybox|sameday|mpl|express one|pick pack)'
        self.__types = [
            {'poi_code': 'hudpdcso', 'poi_common_name': 'DPD', 'poi_type': 'vending_machine_parcel_locker_and_mail_in',
             'poi_tags': hudpdcso, 'poi_url_base': 'https://www.dpd.com', 'poi_search_name': 'dpd',
             'poi_search_avoid_name': avoid_name, 'export_poi_name': False,
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 600, 'osm_search_distance_safe': 250, 'osm_search_distance_unsafe': 2},
            {'poi_code': 'hudpdpp', 'poi_common_name': 'DPD', 'poi_type': 'post_partner',
             'poi_tags': hudpdpp, 'poi_url_base': 'https://www.dpd.com', 'poi_search_name': 'dpd',
             'poi_search_avoid_name': avoid_name, 'export_poi_name': False,
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 600, 'osm_search_distance_safe': 250, 'osm_search_distance_unsafe': 2},
        ]
        return self.__types

    @staticmethod
    def __get_session_and_csrf():
        """Establish a session (cookies) and read the Spring Security CSRF token off the
        parcel-shops page, both required by the search POST below."""
        session = requests.Session(impersonate=IMPERSONATE)
        response = session.get(PARCEL_SHOPS_PAGE_URL, timeout=60)
        response.raise_for_status()
        match = re.search(r'name="_csrf"\s+content="([^"]+)"', response.text)
        if not match:
            raise Exception('Could not find CSRF token on the DPD parcel-shops page')
        return session, match.group(1)

    @classmethod
    def __query(cls, session, csrf_token, lat, lon):
        """Query the 10 shops nearest to (lat, lon). Returns a list of raw shop dicts."""
        body = {
            'latitude': str(lat), 'longitude': str(lon),
            'filterValues': {'typeFilters': [], 'serviceFilters': [], 'openingFilters': 'ANY_TIME',
                             'openingDayFilters': None, 'openingFromFilters': None, 'openingToFilters': None},
            'addressValue': '',
        }
        headers = dict(REQUEST_HEADERS)
        headers['X-CSRF-TOKEN'] = csrf_token
        response = session.post(PARCEL_SHOP_MAP_URL, json=body, headers=headers, timeout=60)
        time.sleep(random.uniform(REQUEST_DELAY_MIN_SECONDS, REQUEST_DELAY_MAX_SECONDS))
        response.raise_for_status()
        return response.json().get('parcelShopDetailsList') or []

    @staticmethod
    def __cell_radius_km(lat, half_lat, half_lon):
        """Approximate distance from a grid cell's center to its edge, in km - the
        smaller of the north-south and east-west half-extents (longitude degrees shrink
        with latitude, hence the cos(lat) term)."""
        return min(half_lat * 111.0, half_lon * 111.0 * math.cos(math.radians(lat)))

    @classmethod
    def __crawl(cls, session, csrf_token, lat, lon, half_lat, half_lon, depth, stores):
        """Fetch (lat, lon)'s nearest shops into `stores` (deduped by id); if they're all
        within this cell's own radius (dense - there may be more just outside it that this
        "nearest 10" query didn't return) and there's recursion budget left, split this
        cell into four quadrants and recurse into each - see the module docstring above."""
        try:
            page = cls.__query(session, csrf_token, lat, lon)
        except Exception as e:
            logging.exception('Exception occurred while fetching DPD grid point %s,%s: %s', lat, lon, e)
            logging.exception(traceback.format_exc())
            return
        for shop in page:
            shop_id = shop.get('id')
            # Hungary borders 7 countries and this is a plain "nearest 10" search with no
            # country filter, so a grid point near/across a border returns genuine foreign
            # DPD shops too (confirmed: ~55% of a full-country crawl's raw results were
            # SK/AT/HR/RO/SI, not HU) - keep only Hungary's own. Still counted towards
            # max_distance below (not filtered out of `page`), since the subdivision
            # decision is about how densely packed the search actually was, regardless of
            # which country the results happen to be in.
            if shop_id and shop_id not in stores and (shop.get('address') or {}).get('country') == 'HU':
                stores[shop_id] = shop
        if not page or depth >= MAX_SUBDIVISION_DEPTH:
            return
        max_distance = max(shop.get('distance', 0) for shop in page)
        if max_distance < cls.__cell_radius_km(lat, half_lat, half_lon):
            quarter_lat, quarter_lon = half_lat / 2, half_lon / 2
            for d_lat in (-quarter_lat, quarter_lat):
                for d_lon in (-quarter_lon, quarter_lon):
                    cls.__crawl(session, csrf_token, round(lat + d_lat, 5), round(lon + d_lon, 5),
                               quarter_lat, quarter_lon, depth + 1, stores)

    @classmethod
    def __fetch_stores(cls):
        """Crawl a national grid (each point the root of a small adaptive quadtree, see
        __crawl()) and return every distinct shop found, deduplicated by id."""
        session, csrf_token = cls.__get_session_and_csrf()
        stores = {}
        (sw_lat, sw_lon), (ne_lat, ne_lon) = HU_BBOX
        half_lat, half_lon = BASE_GRID_STEP_LAT / 2, BASE_GRID_STEP_LON / 2
        lat = sw_lat
        while lat <= ne_lat:
            lon = sw_lon
            while lon <= ne_lon:
                cls.__crawl(session, csrf_token, round(lat, 4), round(lon, 4), half_lat, half_lon, 0, stores)
                lon += BASE_GRID_STEP_LON
            lat += BASE_GRID_STEP_LAT
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
            for shop in stores:
                try:
                    self.data.code = 'hudpdcso' if shop.get('pudoType') == LOCKER_PUDO_TYPE else 'hudpdpp'
                    self.data.branch = clean_string(shop.get('name'))
                    self.data.ref = clean_string(shop.get('id'))

                    position = shop.get('position') or {}
                    self.data.lat, self.data.lon = check_hu_boundary(position.get('lat'), position.get('lon'))

                    address = shop.get('address') or {}
                    self.data.street = clean_string(address.get('streetName'))
                    self.data.housenumber = clean_string(address.get('houseNumber'))
                    self.data.postcode = clean_string(address.get('zipCode'))
                    self.data.city = clean_city(address.get('city'))
                    self.data.original = clean_string('{} {}, {} {}'.format(
                        address.get('streetName'), address.get('houseNumber'),
                        address.get('zipCode'), address.get('city')))

                    business_hours = shop.get('businessHours') or {}
                    for i, day_name in enumerate(DAY_ORDER):
                        intervals = business_hours.get(day_name)
                        if intervals:
                            self.data.day_open(i, intervals[0]['from'][:5])
                            self.data.day_close(i, intervals[0]['to'][:5])
                        else:
                            self.data.day_open_close(i, None, None)

                    self.data.public_holiday_open = not shop.get('onHoliday', False)
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(shop)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
