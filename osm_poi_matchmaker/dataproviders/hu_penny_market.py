# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_penny_market(DataProvider):


    def constains(self):
        self.link = 'https://www.penny.hu/stores-map-data'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hupennysup', 'poi_name': 'Penny Market', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Penny Market Kft.', 'brand': 'Penny Market', 'brand:wikidata': 'Q284688', 'brand:wikipedia': 'en:Penny (supermarket)', 'internet_access': 'wlan', 'internet_access:fee': 'no', 'internet_access:ssid': 'PENNY FREE WLAN', 'contact:email': 'ugyfelszolgalat@penny.hu', 'contact:facebook': 'https://www.facebook.com/PennyMarketMagyarorszag', 'contact:instagram': 'https://www.instagram.com/pennymarkethu', 'contact:youtube': 'https://www.youtube.com/channel/UCSy0KKUrDxVWkx8qicky_pQ', " + POS_HU_GEN + PAY_CASH + "'ref:vatin:hu': '10969629-2-44', 'ref:vatin': 'HU10969629'}",
                 'poi_url_base': 'https://www.penny.hu', 'poi_search_name': 'penny', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 15}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                text = json.loads(soup.get_text())
                for poi_data in text['markets']:
                  self.data.name = 'Penny'
                  self.data.code = 'hupennysup'
                  self.data.postcode = poi_data['address']['zip'].strip()
                  street_tmp = poi_data['address']['street'].split(',')[0]
                  self.data.city = clean_city(poi_data['address']['city'])
                  self.data.original = poi_data['address']['street']
                  self.data.lat, self.data.lon = check_hu_boundary(poi_data['address']['latitude'], poi_data['address']['longitude'])
                  self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                      street_tmp.title())
                  self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                              self.data.postcode)
                  if 'phone' in poi_data and poi_data['phone'] != '':
                      self.data.phone = clean_phone_to_str(poi_data['phone'])
                  if 'id' in poi_data and poi_data['id'] != '':
                      self.data.ref = poi_data['id'].strip()
                  self.data.public_holiday_open = False
                  # TODO: Parsing opening_hours from datasource
                  self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
