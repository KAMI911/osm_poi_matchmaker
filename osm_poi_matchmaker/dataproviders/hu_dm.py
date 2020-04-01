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
        self.link = 'https://services.dm.de/storedata/stores/bbox/49%2C16%2C45%2C23'
        self.POI_COMMON_TAGS = "'shop': 'chemist', 'operator': 'dm Kft.', 'brand': 'dm',  " \
                               "'brand:wikidata': 'Q266572', 'brand:wikipedia': 'en:Dm-drogerie markt', " \
                               "'contact:facebook':'https://www.facebook.com/dm.Magyarorszag', " \
                               "'contact:youtube': 'https://www.youtube.com/user/dmMagyarorszag', " \
                               "'contact:instagram':'https://www.instagram.com/dm_magyarorszag', "
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hudmche', 'poi_name': 'dm', 'poi_type': 'chemist',
                 'poi_tags': "{" + self.POI_COMMON_TAGS + POS_HU_GEN + PAY_CASH + "'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.dm.hu', 'poi_search_name': 'dm',
                         'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
                         'osm_search_distance_unsafe': 15}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                text = json.loads(soup.get_text())
                for poi_data in text['stores']:
                    try:
                        if poi_data.get('localeCountry').strip().upper() == 'HU':
                            self.data.name = 'dm'
                            self.data.code = 'hudmche'
                            self.data.postcode = poi_data.get('address')['zip'].strip()
                            street_tmp = poi_data.get('address')['street'].split(',')[0]
                            self.data.city = clean_city(poi_data.get('address')['city'])
                            self.data.website = 'https://www.dm.hu{}'.format(poi_data.get('storeUrlPath'))
                            self.data.original = poi_data.get('address')['street']
                            self.data.lat, self.data.lon = \
                                check_hu_boundary(poi_data.get('location')['lat'], poi_data.get('location')['lon'])
                            self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                                extract_street_housenumber_better_2(street_tmp.title())
                            if poi_data.get('phone') is not None and poi_data.get('phone') != '':
                                self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                            if poi_data.get('storeNumber') is not None and poi_data.get('storeNumber') != '':
                                self.data.phone = clean_phone_to_str(poi_data.get('storeNumber'))
                            self.data.public_holiday_open = False
                            self.data.add()
                    except Exception as e:
                        logging.error(e)
                        logging.error(poi_data)
                        logging.error(traceback.print_exc())
        except Exception as e:
            logging.error(e)
            logging.error(traceback.print_exc())
