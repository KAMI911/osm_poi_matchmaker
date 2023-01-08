# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_javascript_variable, clean_phone_to_str, \
        clean_email, clean_string, clean_city, clean_street, clean_postcode, clean_branch
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_jysk(DataProvider):

    def contains(self):
        self.link = 'https://jysk.hu/aruhazak'
        self.tags = {'shop': 'furniture', 'brand:wikidata': 'Q138913', 'brand:wikipedia': 'hu:JYSK',
                     'contact:facebook': 'https://www.facebook.com/JYSK.Magyarorszag/',
                     'operator:addr': '1103 Budapest, Sibrik Miklós út 30.', 'operator': 'JYSK Kft.',
                     'ref:vatin': 'HU13353298', 'ref:HU:vatin': '13353298-2-44', 'ref:HU:company': '01-09-730940', }
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hujyskfur = self.tags.copy()
        hujyskfur.update(POS_HU_GEN)
        hujyskfur.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hujyskfur', 'poi_common_name': 'Jysk', 'poi_type': 'furniture',
             'poi_tags': hujyskfur, 'poi_url_base': 'https://jysk.hu', 'poi_search_name': 'jysk',
             'osm_search_distance_perfect': 800, 'osm_search_distance_safe': 300,
             'osm_search_distance_unsafe': 80},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), self.filetype)
            if soup is not None:
                soup_data = soup.find('div', {'data-jysk-react-component': 'StoresLocatorLayout'})['data-jysk-react-properties']
                json_data = json.loads(soup_data, strict=False)
                for shop in json_data.get('storesCoordinates'):
                    try:
                        self.data.code = 'hujyskfur'
                        self.data.lat, self.data.lon = check_hu_boundary(
                            shop.get('lat'), shop.get('lng'))
                        self.data.branch = shop.get('name')
                        internal_id = clean_string(shop.get('id'))
                        self.data.ref = internal_id
                        shop_soup = save_downloaded_soup('{}?storeId={}'.format(self.link, internal_id),
                                                        os.path.join(self.download_cache,
                                                        '{}.{}.json'.format(self.filename, internal_id)), FileType.json)

                        self.data.city = clean_city(shop_soup.get('city'))
                        self.data.postcode = clean_postcode(shop_soup.get('zip'))
                        self.data.street = clean_street(shop_soup.get('street'))
                        self.data.phone = clean_phone_to_str(shop_soup.get('tel'))
                        self.data.housenumber = clean_string(shop_soup.get('house'))
                        self.data.branch = clean_branch(shop_soup.get('shop_name'))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(shop)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
