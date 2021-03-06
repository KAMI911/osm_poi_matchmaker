# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POST_DATA = {'country': 'Magyarország', 'lat': '47.162494',
             'lng': '19.503304100000037', 'radius': 20}


class hu_mol(DataProvider):

    def constains(self):
        self.link = 'http://toltoallomaskereso.mol.hu/hu/portlet/routing/along_latlng.json'
        self.fuel = {'amenity': 'fuel', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes', 'air_conditioning': 'yes'}
        self.tags = {'brand': 'MOL', 'operator': 'MOL Nyrt.',
                     'operator:addr': '1117 Budapest, Október huszonharmadika utca 18.',
                     'ref:vatin:hu': '10625790-4-44',
                     'contact:facebook': 'https://www.facebook.com/mol.magyarorszag/',
                     'contact:youtube': 'https://www.youtube.com/user/molgrouptv',
                     'contact:instagram': 'https://www.instagram.com/mol.magyarorszag/',
                     'brand:wikipedia': 'hu:MOL Magyar Olaj- és Gázipari Nyrt.', 'brand:wikidata': 'Q549181',
                     'ref:HU:company': '01-10-041683'}
        self.waterway_fuel = {'waterway': 'fuel'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        humolfu = self.tags.copy()
        humolfu.update(self.fuel)
        humolfu.update(POS_HU_GEN)
        humolfu.update(PAY_CASH)
        humolwfu = self.tags.copy()
        humolwfu.update(self.waterway_fuel)
        humolwfu.update(POS_HU_GEN)
        humolwfu.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'humolfu', 'poi_name': 'MOL', 'poi_type': 'fuel',
             'poi_tags': humolfu, 'poi_url_base': 'https://www.mol.hu', 'poi_search_name': 'mol',
             'osm_search_distance_perfect': 2000,
             'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 60},
            {'poi_code': 'humolwfu', 'poi_name': 'MOL', 'poi_type': 'fuel',
             'poi_tags': humolwfu, 'poi_url_base': 'https://www.mol.hu', 'poi_search_name': 'mol',
             'osm_search_distance_perfect': 2000,
             'osm_search_distance_safe': 800, 'osm_search_distance_unsafe': 300}, ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype, POST_DATA)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    self.data.name = 'MOL'
                    if " Sziget " in poi_data.get('name'):
                        self.data.code = 'humolwfu'
                    else:
                        self.data.code = 'humolfu'
                    self.data.postcode = poi_data.get('postcode').strip()
                    self.data.city = clean_city(poi_data['city'])
                    self.data.original = poi_data['address']
                    self.data.lat, self.data.lon = check_hu_boundary(
                        poi_data['lat'], poi_data['lng'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data['address'])
                    self.data.public_holiday_open = True
                    self.data.truck = True if 'kamion_parkolo' in poi_data.get(
                        'servicesin') else False
                    self.data.food = True if 'fresh_corner' in poi_data.get(
                        'servicesin') else False
                    self.data.rent_lpg_bottles = True if 'pb' in poi_data.get(
                        'servicesin') else False
                    self.data.fuel_adblue = True if 'adblue' in poi_data.get(
                        'servicesin') else False
                    self.data.fuel_lpg = True if 'lpg' in poi_data.get(
                        'servicesin') else False
                    self.data.fuel_octane_95 = True
                    self.data.fuel_diesel = True
                    self.data.fuel_octane_100 = True
                    self.data.fuel_diesel_gtl = True
                    self.data.compressed_air = True
                    self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            logging.exception('Exception occurred')

            logging.error(e)
