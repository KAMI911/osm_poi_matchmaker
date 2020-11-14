# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours, \
        clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_oil(DataProvider):

    def constains(self):
        self.link = 'http://www.oil-benzinkutak.hu/wp-admin/admin-ajax.php?action=store_search&lat=47.162494&lng=19.5033041&max_results=1&search_radius=50&autoload=1'
        self.tags = {'amenity': 'fuel', 'name': 'OIL!', 'brand': 'OIL!', 'fuel:diesel': 'yes',
                     'fuel:octane_95': 'yes', 'brand:wikidata': 'Q2007561',
                     'brand:wikipedia': 'OIL! Tankstellen', 'operator': 'Mabanaft Hungary Kft.',
                     'operator:addr': '1016 Budapest, Mészáros utca 58/B',
                     'ref:vatin:hu': '12700226-2-44', 'ref:vatin': 'HU12700226',
                     'ref:HU:company': '01-09-699184',
                     'contact:facebook': 'https://www.facebook.com/OILHungary/', }
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        huoilfu = self.tags.copy()
        huoilfu.update(POS_HU_GEN)
        huoilfu.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'huoilfu', 'poi_name': 'OIL!', 'poi_type': 'fuel',
             'poi_tags': huoilfu, 'poi_url_base': 'https://www.oil-benzinkutak.hu', 'poi_search_name': '(oil|oil!|oil benzinkutak|oil-benzinkutak)',
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
                        self.data.name = 'OIL!'
                        self.data.code = 'huoilfu'
                        if poi_data.get('zip') is not None and poi_data.get('zip') != '':
                            self.data.postcode = poi_data.get('zip').strip()
                        if poi_data.get('city') is not None and poi_data.get('city') != '':
                            self.data.city = clean_city(poi_data.get('city'))
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('lat'), poi_data.get('lng'))
                        if poi_data.get('address') is not None and poi_data.get('address') != '':
                            self.data.original = poi_data.get('address')
                            self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                                extract_street_housenumber_better_2(
                                    poi_data.get('address'))
                        if poi_data.get('phone') is not None and poi_data.get('phone') != '':
                            self.data.phone = clean_phone_to_str(
                                poi_data.get('phone'))
                        self.data.fuel_octane_95 = True
                        self.data.fuel_diesel = True
                        if poi_data.get('id') is not None and poi_data.get('id') != '':
                            self.data.ref = poi_data.get('id').strip()
                        if poi_data.get('url') is not None and poi_data.get('url') != '':
                            self.data.website = poi_data.get('url').strip()
                        if poi_data.get('store') is not None and poi_data.get('store') != '':
                            self.data.branch = poi_data.get('store').strip()
                        self.data.add()
                    except Exception as e:
                        logging.error(e)
                        logging.error(poi_data)
                        logging.exception('Exception occurred')

        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')
