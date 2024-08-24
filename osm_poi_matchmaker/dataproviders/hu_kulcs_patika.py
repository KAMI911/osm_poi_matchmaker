# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_city, clean_string, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_kulcs_patika(DataProvider):

    def contains(self):
        self.link = 'https://kulcspatikak.hu/gykeress_feed.php'
        self.tags = {'amenity': 'pharmacy', 'brand': 'Kulcs Patikák',
                     'dispensing': 'yes', 'air_conditioning': 'yes'}
        self.headers = {'Referer': 'https://kulcspatikak.hu/patikakereso',
                        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0',
                        'Accept': 'application/json, text/javascript, */*; q=0.01'}
        self.post = {'megyeid': -1, 'quality_check': 0,
                     'animal_check': 0, 'nonstop_check': 0, 'hetvege_check': 0}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hukulcspha = self.tags.copy()
        hukulcspha.update(POS_HU_GEN)
        hukulcspha.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hukulcspha', 'poi_common_name': 'Kulcs Patika', 'poi_type': 'pharmacy',
             'poi_tags': hukulcspha,
             'poi_url_base': 'https://www.kulcspatikak.hu/', 'poi_search_name': '(kulcs patikák|kulcs patika|kulcs)',
             'preserve_original_name': True},
        ]
        return self.__types

    def process(self):
        try:
            if self.link:
                soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache,
                                            self.filename), self.filetype, False, post_data=self.post,
                                            verify=self.verify_link, headers=self.headers)
                if soup is not None:
                    text = json.loads(soup, strict=False)
                    logging.debug(text)
                    for poi_data in text.get('patikaIdList'):
                        poi_data_poi = poi_data.get('poi')
                        try:
                            if 'Kulcs patika' not in poi_data_poi.get('patika'):
                                self.data.name = clean_string(poi_data_poi.get('patika'))
                                self.data.branch = None
                            else:
                                self.data.branch = clean_string(poi_data_poi.get('patika'))
                            self.data.code = 'hukulcspha'
                            self.data.phone = clean_phone_to_str(poi_data_poi.get('phone'))
                            self.data.lat, self.data.lon = \
                                check_hu_boundary(poi_data_poi.get('latitude'),
                                                  poi_data_poi.get('longitude'))
                            self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                                self.data.conscriptionnumber = extract_all_address_waxeye(
                                    poi_data_poi.get('address'))
                            self.data.public_holiday_open = False
                            self.data.add()
                        except Exception as e:
                            logging.exception('Exception occurred: {}'.format(e))
                            logging.exception(traceback.print_exc())
                            logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
