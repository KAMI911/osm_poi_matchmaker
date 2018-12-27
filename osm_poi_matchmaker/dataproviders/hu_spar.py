# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

PATTERN_SPAR_REF = re.compile('\((.*?)\)')


class hu_spar(DataProvider):


    def constains(self):
        self.link = 'https://www.spar.hu/bin/aspiag/storefinder/stores?country=HU'
        self.POI_COMMON_TAGS = "'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'brand:wikipedia': 'hu:Spar', 'brand:wikidata': 'Q610492', 'addr:country': 'HU', 'email': 'vevoszolgalat@spar.hu', 'phone': '+36208237727', 'facebook': 'https://www.facebook.com/sparmagyarorszag', 'youtube': 'https://www.youtube.com/channel/UC9tu8COHiy4WkeTIN1k_Y8A', 'instagram': 'https://www.instagram.com/sparmagyarorszag', 'payment:cash': 'yes'"
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'husparexp', 'poi_name': 'Spar Expressz', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': 'spar', 'osm_search_distance_safe': 60, 'osm_search_distance_unsafe': 15},
                {'poi_code': 'husparint', 'poi_name': 'Interspar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'payment:contactless': 'yes', 'payment:american_express': 'yes', 'payment:mastercard': 'yes', 'payment:maestro': 'yes', 'payment:v_pay': 'yes', 'payment:visa': 'yes', 'payment:visa_electron': 'yes', 'payment:erzsebet': 'yes', 'payment:erzsebet_plus': 'yes', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': 'spar', 'osm_search_distance_safe': 60, 'osm_search_distance_unsafe': 15},
                {'poi_code': 'husparsup', 'poi_name': 'Spar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'payment:contactless': 'yes', 'payment:american_express': 'yes', 'payment:mastercard': 'yes', 'payment:maestro': 'yes', 'payment:v_pay': 'yes', 'payment:visa': 'yes', 'payment:visa_electron': 'yes', 'payment:erzsebet': 'yes', 'payment:erzsebet_plus': 'yes', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': 'spar', 'osm_search_distance_safe': 60, 'osm_search_distance_unsafe': 15}]
        return self.__types

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                if 'xpres' in poi_data['name']:
                    self.data.name = 'Spar Expressz'
                    self.data.code = 'husparexp'
                elif 'INTER' in poi_data['name']:
                    self.data.name = 'Interspar'
                    self.data.code = 'husparint'
                elif 'market' in poi_data['name']:
                    self.data.name = 'Spar'
                    self.data.code = 'husparsup'
                else:
                    self.data.name = 'Spar'
                    self.data.code = 'husparsup'
                poi_data['name'] = poi_data['name'].replace('INTERSPAR', 'Interspar')
                poi_data['name'] = poi_data['name'].replace('SPAR', 'Spar')
                ref_match = PATTERN_SPAR_REF.search(poi_data['name'])
                self.data.ref = ref_match.group(1).strip() if ref_match is not None else None
                self.data.city = clean_city(poi_data['city'])
                self.data.postcode = poi_data['zipCode'].strip()
                self.data.branch = poi_data['name'].split('(')[0].strip()
                self.data.website = poi_data['pageUrl'].strip()
                self.data.lat, self.data.lon = check_hu_boundary(poi_data['latitude'], poi_data['longitude'])
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                            self.data.postcode)
                self.data.original = poi_data['address']
                self.data.public_holiday_open = False
                self.data.add()
