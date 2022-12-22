# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours, \
        clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_oil(DataProvider):

    def contains(self):
        self.link = 'http://www.oil-benzinkutak.hu/wp-admin/admin-ajax.php?action=store_search&lat=47.162494&lng=19.5033041&max_results=1&search_radius=50&autoload=1'
        self.tags = {'amenity': 'fuel', 'name': 'OIL!', 'brand': 'OIL!', 'fuel:diesel': 'yes',
                     'fuel:octane_95': 'yes', 'brand:wikidata': 'Q2007561',
                     'brand:wikipedia': 'OIL! Tankstellen', 'operator': 'Mabanaft Hungary Kft.',
                     'operator:addr': '1016 Budapest, Mészáros utca 58/B',
                     'ref:vatin:hu': '12700226-2-44', 'ref:vatin': 'HU12700226',
                     'ref:HU:company': '01-09-699184',
                     'contact:facebook': 'https://www.facebook.com/OILHungary/', }
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huoilfu = self.tags.copy()
        huoilfu.update(POS_HU_GEN)
        huoilfu.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'huoilfu', 'poi_common_name': 'OIL!', 'poi_type': 'fuel',
             'poi_tags': huoilfu, 'poi_url_base': 'https://www.oil-benzinkutak.hu',
             'poi_search_name': '(oil|oil!|oil benzinkutak|oil-benzinkutak)',
             'poi_search_avoid_name': '(mol|shell|m. petrol|avia|lukoil|hunoil)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 450,
             'osm_search_distance_unsafe': 60},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    try:
                        self.data.code = 'huoilfu'
                        self.data.postcode = clean_string(poi_data.get('zip'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lng'))
                        self.data.original = clean_string(poi_data.get('address'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                                extract_street_housenumber_better_2(
                                    poi_data.get('address'))
                        self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                        self.data.fuel_octane_95 = True
                        self.data.fuel_diesel = True
                        self.data.ref = clean_string(poi_data.get('id').strip())
                        if poi_data.get('url') is not None and poi_data.get('url') != '':
                            self.data.website = poi_data.get('url').strip()
                        else:
                            self.data.website = 'https://www.oil-benzinkutak.hu'
                        tmp = clean_string(poi_data.get('store').split(' ', 1))
                        self.data.branch = tmp[1].strip().capitalize()
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
