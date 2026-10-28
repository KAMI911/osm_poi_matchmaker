# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# Same AmRest ordering-platform API as hu_kfc.py (see that file for the auth-flow
# rationale) - Pizza Hut Hungary is run by the same franchise operator, just under a
# different brand/country key.
AUTH_URL = 'https://api.amrest.eu/amdv/ordering-api/PH_HU/rest/v1/auth/get-token'
LIST_URL = 'https://api.amrest.eu/amdv/ordering-api/PH_HU/rest/v3/restaurants/'
AUTH_HEADERS = {'Brand': 'PH', 'Source': 'WEB'}
AUTH_BODY = {'deviceUuid': 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF', 'deviceUuidSource': 'FINGERPRINT',
            'source': 'WEB_PH'}

AVOID_NAME = "(burger king|bk|kfc|mcdonald's|mcdonalds|subway|domino's|dominos|burger|hong kong)"


class hu_pizza_hut(DataProvider):
    """Imports Pizza Hut restaurant locations in Hungary from AmRest's ordering-platform
    API (api.amrest.eu/amdv/ordering-api/PH_HU/...), the same one pizzahut.hu's own
    restaurant finder uses. See hu_kfc.py for why only the restaurant-list call
    (not the per-restaurant "details" call) is used."""

    def contains(self):
        self.link = LIST_URL
        self.tags = {'amenity': 'fast_food', 'brand': 'Pizza Hut', 'brand:wikidata': 'Q191615',
                     'cuisine': 'pizza', 'contact:website': 'https://pizzahut.hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huphfaf = self.tags.copy()
        self.__types = [
            {'poi_code': 'huphfaf', 'poi_common_name': 'Pizza Hut', 'poi_type': 'fastfood', 'poi_tags': huphfaf,
             'poi_url_base': 'https://pizzahut.hu', 'poi_search_name': '(pizza hut|pizzahut)',
             'poi_search_avoid_name': AVOID_NAME,
             'additional_ref_name': 'ref', 'osm_search_distance_perfect': 300,
             'osm_search_distance_safe': 100, 'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def __fetch_token(self):
        cache_file = os.path.join(self.download_cache, 'hu_pizza_hut_token.json')
        soup = save_downloaded_soup(AUTH_URL, cache_file, self.filetype, False, AUTH_BODY, headers=AUTH_HEADERS,
                                    json_body=True)
        if soup is None:
            return None
        return json.loads(soup).get('token')

    def process(self):
        try:
            token = self.__fetch_token()
            if token is None:
                logging.warning('Could not obtain an AmRest auth token. Skipping ...')
                return
            headers = {'Authorization': 'Bearer {}'.format(token), 'Brand': 'PH', 'Source': 'WEB'}
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False, headers=headers)
            if soup is None:
                return
            for poi_data in json.loads(soup).get('restaurants', []):
                try:
                    self.data.code = 'huphfaf'
                    self.data.ref = clean_string(poi_data.get('id'))
                    name = clean_string(poi_data.get('name')) or ''
                    self.data.branch = name.removeprefix('Pizza Hut ') or None
                    self.data.city = clean_city(poi_data.get('addressCity'))
                    self.data.postcode = clean_string(poi_data.get('addressPostalCode'))
                    self.data.street = clean_string(poi_data.get('addressStreet'))
                    self.data.housenumber = clean_string(poi_data.get('addressStreetNo'))
                    self.data.original = clean_string('{} {}'.format(
                        poi_data.get('addressStreet') or '', poi_data.get('addressStreetNo') or '').strip())
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('geoLat'), poi_data.get('geoLng'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
