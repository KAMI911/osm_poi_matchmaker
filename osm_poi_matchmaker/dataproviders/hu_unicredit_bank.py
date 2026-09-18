# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from urllib.parse import urlencode
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# UniCredit's shared group locator (also used by the unicreditbank.hu branch finder iframe)
# returns markers inside a map bounding box, but the server collapses dense areas to at
# most ~99 markers. We therefore walk the country as a quadtree: a cell that comes back
# at/above SPLIT_THRESHOLD is subdivided until every leaf is complete.
API = 'https://group.unicreditbanking.net/branch/markerService/getMarkersFiltered'
COUNTRY_BBOX = (45.6, 16.0, 48.7, 23.0)  # (sw_lat, sw_lng, ne_lat, ne_lng) covering Hungary
SPLIT_THRESHOLD = 90  # complete responses observed up to ~95; truncation plateaus at ~99
MIN_SPAN = 0.02  # stop subdividing below this span (degrees) to guarantee termination

AVOID_NAME = '(otp|cib|k&h|kh bank|raiffeisen|erste|mbh|mkb|takarékbank|budapest bank|gránit|sberbank|magnet)'


class hu_unicredit_bank(DataProvider):
    """Imports UniCredit Bank branch/ATM locations in Hungary from the same map marker
    API (group.unicreditbanking.net/branch/markerService/getMarkersFiltered) the bank's
    own branch/ATM finder uses, recursively subdividing the search area to work around
    the API's ~99-marker-per-request cap."""

    def contains(self):
        self.link = API
        self.tags = {'brand': 'UniCredit', 'brand:wikidata': 'Q45568',
                     'contact:website': 'https://www.unicreditbank.hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hunicredbk = {'amenity': 'bank', 'atm': 'yes'}
        hunicredbk.update(self.tags)
        hunicredat = {'amenity': 'atm'}
        hunicredat.update(self.tags)
        self.__types = [
            {'poi_code': 'hunicredbk', 'poi_common_name': 'UniCredit', 'poi_type': 'bank', 'poi_tags': hunicredbk,
             'poi_url_base': 'https://www.unicreditbank.hu', 'poi_search_name': '(unicredit bank|unicredit)',
             'poi_search_avoid_name': AVOID_NAME, 'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'hunicredat', 'poi_common_name': 'UniCredit ATM', 'poi_type': 'atm', 'poi_tags': hunicredat,
             'poi_url_base': 'https://www.unicreditbank.hu', 'poi_search_name': '(unicredit bank|unicredit)',
             'poi_search_avoid_name': AVOID_NAME, 'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def __fetch_cell(self, bbox):
        sw_lat, sw_lng, ne_lat, ne_lng = bbox
        params = {'country': 'HU', 'lang': 'hu', 'mandant': 'hu', 'globalFilter': 3, 'localFilter': '',
                  'showGroupLocations': 'false', 'swLat': sw_lat, 'swLng': sw_lng, 'neLat': ne_lat,
                  'neLng': ne_lng, 'zoomLevel': 13}
        cache_name = 'hu_unicredit_cell_{:.4f}_{:.4f}_{:.4f}_{:.4f}.json'.format(*bbox)
        url = '{}?{}'.format(self.link, urlencode(params))
        soup = save_downloaded_soup(url, os.path.join(self.download_cache, cache_name), self.filetype, False)
        if soup is None:
            return []
        try:
            return json.loads(soup)
        except (json.JSONDecodeError, ValueError):
            return []

    def __walk(self, bbox):
        locations = self.__fetch_cell(bbox)
        sw_lat, sw_lng, ne_lat, ne_lng = bbox

        if len(locations) >= SPLIT_THRESHOLD and (ne_lat - sw_lat) > MIN_SPAN and (ne_lng - sw_lng) > MIN_SPAN:
            mid_lat, mid_lng = (sw_lat + ne_lat) / 2, (sw_lng + ne_lng) / 2
            for sub_bbox in ((sw_lat, sw_lng, mid_lat, mid_lng), (sw_lat, mid_lng, mid_lat, ne_lng),
                             (mid_lat, sw_lng, ne_lat, mid_lng), (mid_lat, mid_lng, ne_lat, ne_lng)):
                for location in self.__walk(sub_bbox):
                    yield location
            return

        if len(locations) >= SPLIT_THRESHOLD:
            logging.warning('UniCredit cell %s hit the marker cap at minimum span; results may be truncated.', bbox)

        for location in locations:
            yield location

    @staticmethod
    def __parse_address(address_html):
        """Split UniCredit's <br/>-joined address blob into (street, postcode, city).
        The last line is always "<postcode> <city>"; a leading "Felújítás miatt ..."
        temporary-relocation notice (a full sentence, not a street) is dropped."""
        if not address_html:
            return None, None, None
        lines = [line.strip() for line in re.split(r'<br\s*/?>', address_html) if line.strip()]
        if not lines:
            return None, None, None
        postcode, city = None, None
        match = re.match(r'(\d{4})\s+(.+?)\.?$', lines[-1])
        if match:
            postcode, city = match.group(1), match.group(2)
            lines = lines[:-1]
        lines = [line for line in lines if 'Felújítás' not in line]
        street = lines[0] if lines else None
        return street, postcode, city

    def process(self):
        try:
            for poi_data in self.__walk(COUNTRY_BBOX):
                try:
                    location_type = poi_data.get('type')
                    if location_type == 'branch':
                        self.data.code = 'hunicredbk'
                        self.data.public_holiday_open = False
                    elif location_type == 'atm':
                        self.data.code = 'hunicredat'
                        self.data.public_holiday_open = True
                    else:
                        logging.debug('Unexpected UniCredit location type: %s', location_type)
                        continue
                    self.data.branch = clean_string(poi_data.get('title'))
                    self.data.ref = clean_string(poi_data.get('id'))
                    street, postcode, city = self.__parse_address(poi_data.get('address'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(street)
                    self.data.postcode = clean_string(postcode)
                    self.data.city = clean_city(city)
                    self.data.original = clean_string(poi_data.get('addressOnly'))
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('locLat'), poi_data.get('locLng'))
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
