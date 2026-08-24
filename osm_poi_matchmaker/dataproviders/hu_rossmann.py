# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    import requests
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_opening_hours, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.enums import WeekDaysLong
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# The uzletkereso page used to embed the store list as Next.js page props, but the site (now on
# shop.rossmann.hu) fetches it client-side from the storefront's own GraphQL API instead.
GRAPHQL_URL = 'https://api.rossmann.hu/graphql'
STORES_QUERY = '''
query stores {
    stores {
        id
        name
        zip_code
        city
        lat
        lng
        street
        address
        openings
        services
        is_open
    }
}
'''


class hu_rossmann(DataProvider):

    def contains(self):
        self.link = GRAPHQL_URL
        self.tags = {'shop': 'chemist', 'operator': 'Rossmann Magyarország Kft.',
                     'operator:addr': '2225 Üllő, Zsaróka út 8.', 'ref:HU:vatin': '11149769-2-44',
                     'ref:vatin': 'HU11149769', 'brand': 'Rossmann', 'brand:wikidata': 'Q316004',
                     'brand:wikipedia': 'de:Dirk Rossmann GmbH', 'contact:email': 'ugyfelszolgalat@rossmann.hu',
                     'contact:phone': '+36 29 889 800', 'contact:mobile': '+36 70 469 2800',
                     'contact:facebook': 'https://www.facebook.com/Rossmann.hu',
                     'contact:youtube': 'https://www.youtube.com/channel/UCmUCPmvMLL3IaXRBtx7-J7Q',
                     'contact:instagram': 'https://www.instagram.com/rossmann_hu', 'air_conditioning': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hurossmche = self.tags.copy()
        hurossmche.update(POS_HU_GEN)
        hurossmche.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hurossmche', 'poi_common_name': 'Rossmann', 'poi_type': 'chemist',
             'poi_tags': hurossmche, 'poi_url_base': 'https://www.rossmann.hu', 'poi_search_name': 'rossmann',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 3},
        ]
        return self.__types

    def process(self):
        try:
            cache_file = os.path.join(self.download_cache, self.filename)
            pois = None
            if config.get_download_use_cached_data() is True and os.path.isfile(cache_file):
                with open(cache_file, mode='r', encoding='utf-8') as file:
                    pois = json.load(file)
            else:
                try:
                    response = requests.post(GRAPHQL_URL, json={'query': STORES_QUERY}, timeout=60)
                    response.raise_for_status()
                    pois = response.json().get('data', {}).get('stores')
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                if pois is not None:
                    if not os.path.exists(self.download_cache):
                        os.makedirs(self.download_cache)
                    with open(cache_file, mode='w', encoding='utf-8') as file:
                        json.dump(pois, file)
            if pois is not None:
                for poi_data in pois:
                    try:
                        # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                        self.data.code = 'hurossmche'
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lng'))
                        self.data.postcode = clean_string(poi_data.get('zip_code'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                                poi_data.get(('street')))
                        opening_hours_raw = poi_data.get('openings')
                        if opening_hours_raw is not None:
                            opening_hours_dict = opening_hours_raw.split("\n")
                            for i in range(0, 7):
                                opening, closing = clean_opening_hours(opening_hours_dict[i])
                                if opening is not None and closing is not None:
                                    self.data.day_open_close(i, opening, closing)
                                else:
                                    self.data.day_open_close(i, None, None)
                        self.data.original = clean_string(poi_data.get(('address')))
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
