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


class hu_jysk(DataProvider):


    def constains(self):
        self.link = 'https://jysk.hu/aruhazak'
        self.POI_COMMON_TAGS = "'brand:wikidata': 'Q138913', 'brand:wikipedia': 'hu:JYSK', " \
            "'contact:facebook': 'https://www.facebook.com/JYSK.Magyarorszag/', " \
            "'operator:addr': '1103 Budapest, Sibrik Miklós út 30.', 'operator': 'JYSK Kft.', " \
            "'ref:vatin': 'HU13353298', 'ref:vatin:hu': '13353298-2-44', 'ref:HU:company': '01 09 730940', " \
            + POS_HU_GEN + PAY_CASH
        self.filename = self.filename + 'html'

    def types(self):
        self.__type = [{'poi_code': 'hujyskfur', 'poi_name': 'Jysk', 'poi_type': 'furniture',
                 'poi_tags': "{'shop': 'furniture', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://jysk.hu', 'poi_search_name': 'jysk', 'osm_search_distance_perfect': 800,
                 'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 80}]
        return self.__type

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                soup_data = soup.find('script', {'data-drupal-selector': 'drupal-settings-json'})
                json_data = json.loads(soup_data.text)
                for shop in json_data['storesLocator']['BuildCoordinates']:
                    self.data.name = 'Jysk'
                    self.data.code = 'hujyskfur'
                    self.data.lat, self.data.lon = check_hu_boundary(shop.get('lat'), shop.get('lon'))
                    self.data.branch = shop.get('name')
                    internal_id = shop.get('id')
                    shop_soup = save_downloaded_soup('{}?storeId={}'.format(self.link, internal_id),
                        os.path.join(self.download_cache, '{}.{}.html'.format(self.filename, internal_id)))
                    self.data.phone = '+36 1 700 8400'
                    self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
