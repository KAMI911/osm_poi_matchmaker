# -*- coding: utf-8 -*-

try:
    from builtins import Exception, ImportError, range
    import logging
    import sys
    import json
    import os
    import re
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, extract_street_housenumber_better_2, clean_phone_to_str, \
        extract_javascript_variable, clean_string, clean_url
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_mobil_petrol(DataProvider):

    def contains(self):
        self.link = 'http://www.mpetrol.hu/'
        self.tags = {'amenity': 'fuel', 'brand': 'Mobil Petrol', 'contact:email': 'info@mpetrol.hu',
                     'contact:facebook': 'https://www.facebook.com/mpetrolofficial/', 'name': 'Mobil Petrol',
                     'operator:addr': '1095 Budapest, Ipar utca 2.', 'operator': 'MPH Power Zrt.', 'fuel:diesel': 'yes',
                     'fuel:octane_95': 'yes'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        humobpefu = self.tags.copy()
        humobpefu.update(POS_HU_GEN)
        humobpefu.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'humobpefu', 'poi_common_name': 'Mobil Petrol', 'poi_type': 'fuel',
             'poi_tags': humobpefu, 'poi_url_base': 'http://mpetrol.hu/',
             'poi_search_name': '(mobil metrol|shell)',
             'poi_search_avoid_name': '(mol|shell|avia|lukoil|hunoil)'}
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                text = json.loads(
                    extract_javascript_variable(soup, 'totem_stations'))
                for poi_data in text.values():
                    try:
                        self.data.name = 'Mobil Petrol'
                        self.data.code = 'humobpefu'
                        self.data.website = clean_url(poi_data.get('description'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.original = clean_string(poi_data.get('address'))
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('location').get('lat'),
                                                                         poi_data.get('location').get('lng'))
                        self.data.postcode = None
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(poi_data.get('address'))
                        self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                        self.data.public_holiday_open = False
                        if '0-24' in poi_data.get('services'):
                            self.data.nonstop = True
                            self.data.public_holiday_open = True
                        else:
                            if '6-22' in poi_data.get('services'):
                                open_from = '06:00'
                                open_to = '22:00'
                            elif '6-21' in poi_data.get('services'):
                                open_from = '06:00'
                                open_to = '21:00'
                            elif '5-22' in poi_data.get('services'):
                                open_from = '05:00'
                                open_to = '22:00'
                            elif '6-18' in poi_data.get('services'):
                                open_from = '06:00'
                                open_to = '18:00'
                            if 'open_from' in locals() and 'open_to' in locals():
                                for i in range(0, 7):
                                    self.data.day_open(i, open_from)
                                    self.data.day_close(i, open_to)
                            self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
