# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import os
    import re
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_javascript_variable, clean_phone_to_str, \
        clean_email
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_avia(DataProvider):


    def constains(self):
        self.link = 'https://jysk.hu/aruhazak'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'html'

    def types(self):
        self.__type = [{'poi_code': 'hujyskfu', 'poi_name': 'Jysk', 'poi_type': 'furniture',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Avia', 'operator': 'AVIA Hungária Kft.', " + POS_HU_GEN + PAY_CASH + "'fuel:diesel': 'yes', 'fuel:octane_95': 'yes', 'contact:email': 'avia@avia.hu', 'contact:facebook': 'https://www.facebook.com/AVIAHungary', 'contact:youtube': 'https://www.youtube.com/channel/UCjvjkjf2RgmKBuTnKSXk-Rg', }",
                 'poi_url_base': 'https://www.avia.hu', 'poi_search_name': 'avia', 'osm_search_distance_perfect': 30000, 'osm_search_distance_safe': 800, 'osm_search_distance_unsafe': 110}]
        return self.__type

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                soup_data = soup.findAll('script', {'data-drupal-selector': 'drupal-settings-json'})
                json_data = json.loads(soup_data)[0].text
                for shop in json_data['storesLocator']['BuildCoordinates']:
                    self.data.lat, self.data.lon = check_hu_boundary(shop.get('lat'), shop.get('lon'))
                    self.data.branch = shop.get('name')
                    internal_id = shop.get('id')
                    shop_soup = save_downloaded_soup('{}?storeId={}'.format(self.link, internal_id),
                        os.path.join(self.download_cache, '{}.{}.html'.format(self.filename, internal_id)))
                    print(shop)
                    continue
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
