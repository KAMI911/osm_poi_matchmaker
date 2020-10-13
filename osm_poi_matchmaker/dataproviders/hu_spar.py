# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_OTP, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception("Exception occurred")
    sys.exit(128)

PATTERN_SPAR_REF = re.compile('\((.*?)\)')


class hu_spar(DataProvider):


    def constains(self):
        self.link = 'https://www.spar.hu/uzletek/_jcr_content.stores.v2'
        self.POI_COMMON_TAGS = "'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar'," \
            "'brand:wikipedia': 'hu:Spar', 'brand:wikidata': 'Q610492',  'contact:email': 'vevoszolgalat@spar.hu', " \
            "'phone': '+36208237727', 'contact:facebook': 'https://www.facebook.com/sparmagyarorszag', " \
            "'contact:youtube': 'https://www.youtube.com/channel/UC9tu8COHiy4WkeTIN1k_Y8A', " \
            "'contact:instagram': 'https://www.instagram.com/sparmagyarorszag', " + POS_OTP + PAY_CASH
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = \
               [{'poi_code': 'husparecon', 'poi_name': 'Spar Expressz', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': '(spar|spar express|spar expressz)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 15},
                {'poi_code': 'husparisup', 'poi_name': 'Interspar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': '(spar|interspar)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 15},
                {'poi_code': 'husparsup', 'poi_name': 'Spar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': '(spar|spar supermarket)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 15},
                {'poi_code': 'huspardcon', 'poi_name': 'DeSpar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': '(spar|despar)',
                 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
                 'osm_search_distance_unsafe': 15},
                        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                    if 'xpres' in poi_data['name']:
                        self.data.name = 'Spar Expressz'
                        self.data.code = 'husparecon'
                    elif 'INTER' in poi_data['name']:
                        self.data.name = 'Interspar'
                        self.data.code = 'husparisup'
                    elif 'market' in poi_data['name']:
                        self.data.name = 'Spar'
                        self.data.code = 'husparsup'
                    elif 'DESPAR' in poi_data['name']:
                        self.data.name = 'DeSpar'
                        self.data.code = 'huspardcon'
                    else:
                        self.data.name = 'Spar'
                        self.data.code = 'husparsup'
                    poi_data['name'] = poi_data['name'].replace('INTERSPAR', 'Interspar')
                    poi_data['name'] = poi_data['name'].replace('SPAR', 'Spar')
                    ref_match = PATTERN_SPAR_REF.search(poi_data['name'])
                    self.data.ref = ref_match.group(1).strip() if ref_match is not None else None
                    self.data.city = clean_city(poi_data['city'])
                    self.data.postcode = poi_data.get('zipCode').strip()
                    self.data.branch = poi_data['name'].split('(')[0].strip()
                    self.data.website = poi_data['pageUrl'].strip()
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['latitude'], poi_data['longitude'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data['address'])
                    self.data.original = poi_data['address']
                    self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            logging.exception("Exception occurred")
            logging.error(e)
