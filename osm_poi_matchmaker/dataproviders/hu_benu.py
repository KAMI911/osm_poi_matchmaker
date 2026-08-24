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
        clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# The old REST endpoint (benu.hu/rest/V1/enabledPharmacySearch) is gone; the storefront now serves
# the pharmacy list as Shopify metaobjects through the public Storefront GraphQL API. The access
# token below is the same public, client-side token embedded in https://benu.hu/pages/patikakereso.
GRAPHQL_URL = 'https://benu-hu-prod.myshopify.com/api/2025-10/graphql.json'
STOREFRONT_ACCESS_TOKEN = 'aceebc583e463763f5c7eaff7a053898'
PHARMACIES_QUERY = '''
query fetchPharmacies($cursor: String) {
    metaobjects(type: "pharmacies", first: 250, after: $cursor) {
        edges {
            node {
                fields { key value }
            }
        }
        pageInfo { hasNextPage endCursor }
    }
}
'''


class hu_benu(DataProvider):

    def contains(self):
        self.link = GRAPHQL_URL
        self.tags = {'brand': 'Benu gyógyszertár', 'dispensing': 'yes',
                     'contact:facebook': 'https://www.facebook.com/BENUgyogyszertar',
                     'contact:youtube': 'https://www.youtube.com/channel/UCBLjL10QMtRHdkak0h9exqg',
                     'air_conditioning': 'yes', }
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hubenupha = {'amenity': 'pharmacy'}
        hubenupha.update(self.tags)
        self.__types = [
            {'poi_code': 'hubenupha', 'poi_common_name': 'Benu gyógyszertár', 'poi_type': 'pharmacy',
             'poi_tags': hubenupha, 'poi_url_base': 'https://benu.hu',
             'poi_search_name': '(benu gyogyszertár|benu)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 20, 'preserve_original_name': True},
        ]
        return self.__types

    def __fetch_pharmacies(self):
        """Fetch every active pharmacy metaobject from the Storefront GraphQL API, following cursor
        pagination (the API caps a single page at 250 results)."""
        headers = {'Content-Type': 'application/json',
                   'X-Shopify-Storefront-Access-Token': STOREFRONT_ACCESS_TOKEN}
        pharmacies = []
        cursor = None
        has_next_page = True
        while has_next_page:
            response = requests.post(GRAPHQL_URL, headers=headers,
                                     json={'query': PHARMACIES_QUERY, 'variables': {'cursor': cursor}},
                                     timeout=60)
            response.raise_for_status()
            page = response.json()['data']['metaobjects']
            for edge in page['edges']:
                fields = {field['key']: field['value'] for field in edge['node']['fields']}
                if fields.get('is_active') == 'true':
                    pharmacies.append(fields)
            has_next_page = page['pageInfo']['hasNextPage']
            cursor = page['pageInfo']['endCursor']
        return pharmacies

    def process(self):
        try:
            cache_file = os.path.join(self.download_cache, self.filename)
            if config.get_download_use_cached_data() is True and os.path.isfile(cache_file):
                with open(cache_file, mode='r', encoding='utf-8') as file:
                    pharmacies = json.load(file)
            else:
                pharmacies = self.__fetch_pharmacies()
                if not os.path.exists(self.download_cache):
                    os.makedirs(self.download_cache)
                with open(cache_file, mode='w', encoding='utf-8') as file:
                    json.dump(pharmacies, file)
            for poi_data in pharmacies or []:
                try:
                    name = clean_string(poi_data.get('name'))
                    if name is not None and 'BENU Gyógyszertár' not in name:
                        self.data.name = name
                        self.data.branch = None
                    else:
                        self.data.branch = name
                    self.data.code = 'hubenupha'
                    self.data.website = None
                    self.data.postcode = clean_string(poi_data.get('zip_code'))
                    self.data.city = clean_city(poi_data.get('city'))
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('latitude'),
                                                                     poi_data.get('longitude'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(poi_data.get('address'))
                    self.data.original = poi_data.get('address')
                    self.data.phone = clean_phone_to_str(poi_data.get('phone_number'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
