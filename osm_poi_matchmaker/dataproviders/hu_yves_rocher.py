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
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_dm(DataProvider):


    def constains(self):
        #self.link = 'https://www.dm.hu/cms/restws/stores/find?requestingCountry=HU&countryCodes=DE%2CAT%2CBA%2CBG%2CSK%2CRS%2CHR%2CCZ%2CRO%2CSI%2CHU%2CMK%2CIT&mandantId=870&bounds=46.599301%2C17.325265%7C47.71978%2C21.681344&before=false&after=false&morningHour=9&eveningHour=18&_=1527413070144'
        self.link = 'https://services.dm.de/storedata/stores/bbox/49%2C16%2C45%2C23'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hudmche', 'poi_name': 'dm', 'poi_type': 'chemist',
                 'poi_tags': "{'shop': 'chemist', 'operator': 'dm Kft.', 'brand': 'dm',  'brand:wikidata': 'Q266572', 'brand:wikipedia': 'en:Dm-drogerie markt', 'contact:facebook':'https://www.facebook.com/dm.Magyarorszag', 'contact:youtube': 'https://www.youtube.com/user/dmMagyarorszag', 'contact:instagram':'https://www.instagram.com/dm_magyarorszag', " + POS_HU_GEN + PAY_CASH + "'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.dm.hu', 'poi_search_name': 'dm', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 15}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                text = json.loads(soup.get_text())
                for poi_data in text['stores']:
                    if poi_data['localeCountry'].strip().upper() == 'HU':
                      self.data.name = 'dm'
                      self.data.code = 'hudmche'
                      self.data.postcode = poi_data['address']['zip'].strip()
                      street_tmp = poi_data['address']['street'].split(',')[0]
                      self.data.city = clean_city(poi_data['address']['city'])
                      self.data.original = poi_data['address']['street']
                      self.data.lat, self.data.lon = check_hu_boundary(poi_data['location']['lat'], poi_data['location']['lon'])
                      self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                          street_tmp.title())
                      if 'phone' in poi_data and poi_data['phone'] != '':
                          self.data.phone = clean_phone_to_str(poi_data['phone'])
                      if 'storeNumber' in poi_data and poi_data['storeNumber'] != '':
                          self.data.ref = poi_data['storeNumber'].strip()
                      self.data.public_holiday_open = False
                      self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)