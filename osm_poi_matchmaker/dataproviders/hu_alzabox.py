# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import re
    import time
    import random
    import traceback
    from curl_cffi import requests
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_string, clean_url
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# alza.hu's storefront (www.alza.hu/uzletek-es-alzaboxok-listaja) drives its pickup-point
# map/list from this JSON search API, found by intercepting the page's own fetch() calls in
# a real browser - a plain HTTP request gets a Cloudflare JS challenge (confirmed: 403 with
# a 'cf-mitigated: challenge' response header regardless of a browser-like User-Agent), so
# this uses curl_cffi with Firefox TLS/HTTP2 impersonation, same as hu_dpd.py/hu_tesco.py.
API_URL = 'https://www.alza.hu/api/salesNetwork/v1/places'
IMPERSONATE = 'firefox135'
ALZABOX_TYPE = 1
# Roughly the centroid of Hungary. Unlike hu_dpd.py's endpoint (always the nearest 10 shops,
# needing a national grid crawl), this one returns every match within `radius` of the search
# point, and paging.size stays at exactly 808 for any radius from 300 to 1000 km - the
# alza.hu storefront's own catalog is already scoped to its Hungarian AlzaBox network, so a
# single sufficiently-large-radius search from the centroid already covers the whole country.
CENTER_LAT, CENTER_LON = 47.1625, 19.5033
SEARCH_RADIUS_M = 400000
PAGE_LIMIT = 100  # server-enforced max ('The field Limit must be between 0 and 100.')
# Be polite and avoid an obvious burst-of-identical-requests pattern.
REQUEST_DELAY_MIN_SECONDS = 0.3
REQUEST_DELAY_MAX_SECONDS = 0.8

# addressText comes as "<street> <no>., <postcode> <city>[ <roman numeral>.]" - street
# first, then postcode+city, optionally with a bare Budapest district numeral suffix
# (no "kerület" word, unlike hu_raiffeisen.py's source) - the reverse of what
# extract_all_address_waxeye expects, and its grammar also doesn't parse en-dash
# housenumber ranges. Confirmed via a live pull: without this, street/housenumber came
# back None for all 809 places; with it, 807/809 parse (the other 2 are bare cadastral
# lot numbers with no street name to extract).
ADDRESS_DISTRICT_SUFFIX_RE = re.compile(r'\s+[IVXLCDM]+\.\s*$')


def _reorder_address(address_text):
    if not address_text:
        return address_text
    normalized = ADDRESS_DISTRICT_SUFFIX_RE.sub('', address_text.strip()).replace('–', '-').replace('—', '-')
    if ',' not in normalized:
        return normalized
    street_part, _, tail = normalized.rpartition(',')
    return '{}, {}'.format(tail.strip(), street_part.strip())


class hu_alzabox(DataProvider):
    """Imports AlzaBox parcel locker locations in Hungary from alza.hu's pickup-place
    search JSON API."""

    def contains(self):
        self.link = API_URL
        self.tags = {
            'brand': 'AlzaBox', 'brand:wikidata': 'Q115254158',
            'operator': 'Alza.hu Kft.', 'operator:addr': '1134 Budapest, Róbert Károly körút 54-58.',
            'ref:vatin': 'HU25745849', 'ref:HU:vatin': '25745849-2-41', 'ref:HU:company': '01-09-286873',
            'contact:email': 'info@alza.hu', 'contact:phone': '+36 1 701 1111',
            'payment:cash': 'no',
        }
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hualzabcso = {'amenity': 'parcel_locker', 'parcel_mail_in': 'yes', 'parcel_pickup': 'yes',
                      'colour': 'white', 'material': 'metal', 'refrigerated': 'no'}
        hualzabcso.update(POS_HU_GEN)
        hualzabcso.update(self.tags)
        self.__types = [
            {'poi_code': 'hualzabcso', 'poi_common_name': 'AlzaBox',
             'poi_type': 'vending_machine_parcel_locker_and_mail_in',
             'poi_tags': hualzabcso, 'poi_url_base': 'https://www.alza.hu', 'poi_search_name': 'alzabox',
             'poi_search_avoid_name': '(dpd|gls|foxpost|packeta|z-box|z-pont|postapont|easybox|sameday|mpl|express one|pick pack)',
             'export_poi_name': False,
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 600, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 2},
        ]
        return self.__types

    @classmethod
    def __fetch_places(cls):
        """Page through the pickup-place search API for AlzaBox (type=1) locations,
        deduplicated by id."""
        session = requests.Session(impersonate=IMPERSONATE)
        places = {}
        offset = 0
        while True:
            params = {
                'types[0]': ALZABOX_TYPE, 'latitude': CENTER_LAT, 'longitude': CENTER_LON,
                'ordering': 0, 'radius': SEARCH_RADIUS_M, 'limit': PAGE_LIMIT, 'offset': offset,
            }
            response = session.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            pickup_places = response.json().get('pickupPlaces') or {}
            page = pickup_places.get('value') or []
            for place in page:
                place_id = place.get('id')
                if place_id:
                    places[place_id] = place
            size = (pickup_places.get('paging') or {}).get('size', 0)
            time.sleep(random.uniform(REQUEST_DELAY_MIN_SECONDS, REQUEST_DELAY_MAX_SECONDS))
            offset += PAGE_LIMIT
            if not page or offset >= size:
                break
        return list(places.values())

    def process(self):
        try:
            cache_file = os.path.join(self.download_cache, self.filename)
            if config.get_download_use_cached_data() is True and os.path.isfile(cache_file):
                with open(cache_file, mode='r', encoding='utf-8') as file:
                    places = json.load(file)
            else:
                places = self.__fetch_places()
                if not os.path.exists(self.download_cache):
                    os.makedirs(self.download_cache)
                with open(cache_file, mode='w', encoding='utf-8') as file:
                    json.dump(places, file)
            for poi_data in places:
                try:
                    self.data.code = 'hualzabcso'
                    self.data.branch = clean_string(poi_data.get('name'))
                    self.data.ref = clean_string(poi_data.get('parcelShopId'))
                    self.data.website = clean_url(poi_data.get('articleUrl'))

                    position = poi_data.get('gpsPosition') or {}
                    self.data.lat, self.data.lon = check_hu_boundary(
                        position.get('latitude'), position.get('longitude'))

                    self.data.original = poi_data.get('addressText')
                    self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                        self.data.conscriptionnumber = extract_all_address_waxeye(
                            _reorder_address(poi_data.get('addressText')))

                    # AlzaBox lockers are outdoor, self-service units, accessible around the
                    # clock regardless of the host business's own opening hours (confirmed via
                    # per-location detail lookups across mall-attached and street-standalone
                    # boxes alike - every one came back "Nonstop").
                    self.data.nonstop = True
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
