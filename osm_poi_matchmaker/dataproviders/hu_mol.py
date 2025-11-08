# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POST_DATA = {'api': 'stations', 'input': 'HU',
             'lang': 'hu', 'mode': 'country'}


class hu_mol(DataProvider):

    def contains(self):
        # self.link = 'https://toltoallomaskereso.mol.hu/api.php'
        self.link = os.path.join(config.get_directory_cache_url(), 'hu_mol.json')
        self.headers = {'Referer': 'https://toltoallomaskereso.mol.hu/hu',
                        'Origin': 'https://toltoallomaskereso.mol.hu',
                        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0',
                        'Accept': '*/*'}
        self.fuel = {'amenity': 'fuel', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes', 'air_conditioning': 'yes'}
        self.tags = {'brand': 'MOL', 'operator': 'MOL Nyrt.',
                     'operator:addr': '1117 Budapest, Október huszonharmadika utca 18.',
                     'ref:HU:vatin': '10625790-4-44',
                     'contact:facebook': 'https://www.facebook.com/mol.magyarorszag/',
                     'contact:youtube': 'https://www.youtube.com/user/molgrouptv',
                     'contact:instagram': 'https://www.instagram.com/mol.magyarorszag/',
                     'brand:wikipedia': 'hu:MOL Magyar Olaj- és Gázipari Nyrt.', 'brand:wikidata': 'Q549181',
                     'ref:HU:company': '01-10-041683'}
        self.waterway_fuel = {'waterway': 'fuel'}
        self.fastfood = {'amenity': 'fastfood'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        humolfu = self.tags.copy()
        humolfu.update(self.fuel)
        humolfu.update(POS_HU_GEN)
        humolfu.update(PAY_CASH)
        humolwfu = self.tags.copy()
        humolwfu.update(self.waterway_fuel)
        humolwfu.update(POS_HU_GEN)
        humolwfu.update(PAY_CASH)
        humolfaf = self.tags.copy()
        humolfaf.update(self.fastfood)
        humolfaf.update(POS_HU_GEN)
        humolfaf.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'humolfu', 'poi_common_name': 'MOL', 'poi_type': 'fuel',
             'poi_tags': humolfu, 'poi_url_base': 'https://www.mol.hu', 'poi_search_name': 'mol',
             'poi_search_avoid_name': '(shell|m. petrol|avia|lukoil|hunoil)',
             'osm_search_distance_perfect': 2000,
             'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 60},
            {'poi_code': 'humolwfu', 'poi_common_name': 'MOL', 'poi_type': 'waterway_fuel',
             'poi_tags': humolwfu, 'poi_url_base': 'https://www.mol.hu', 'poi_search_name': 'mol',
             'poi_search_avoid_name': '(shell|m. petrol|avia|lukoil|hunoil)',
             'osm_search_distance_perfect': 2000,
             'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 100},
            {'poi_code': 'humolfaf', 'poi_common_name': 'Fresh Corner', 'poi_type': 'fastfood',
             'poi_tags': humolfaf, 'poi_url_base': 'https://www.mol.hu', 'poi_search_name': 'fresh corner',
             'poi_search_avoid_name': '(étterem|bistro|bisztr|csárda|bár)',
             'osm_search_distance_perfect': 200,
             'osm_search_distance_safe': 100, 'osm_search_distance_unsafe': 10}, ]
        return self.__types

    def process(self):
        try:
            # soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
            #                            self.filetype, False, post_data=POST_DATA, headers=self.headers)
            # if soup is not None:
            with open(self.link, 'r') as f:
                text = json.load(f)
                # text = json.loads(soup)
                for poi_data in text:
                    try:
                        if ' Sziget ' in poi_data.get('name'):
                            self.data.code = 'humolwfu'
                        else:
                            if 'FRESH_CORNER' in poi_data.get('services') and \
                              not ('SHOP' in poi_data.get('services') or \
                              ('AD_BLUE' in poi_data.get('services') or \
                              ('TOLL_TERMINAL' in poi_data.get('services')))):
                                self.data.code = 'humolfaf'
                            else:
                                self.data.code = 'humolfu'
                        self.data.postcode = clean_string(poi_data.get('postcode'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.original = clean_string(poi_data.get('address'))
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('gpsPosition').get('latitude'), poi_data.get('gpsPosition').get('longitude'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            poi_data['address'])
                        self.data.public_holiday_open = True
                        self.data.truck = True if 'TRUCK_PARK' in poi_data.get(
                            'services') else False
                        self.data.food = True if 'FRESH_CORNER' in poi_data.get(
                            'services') else False
                        self.data.rent_lpg_bottles = True if 'CYLINDER_PB_GAS' in poi_data.get(
                            'services') else False
                        self.data.fuel_adblue = True if 'AD_BLUE' in poi_data.get(
                            'services') else False
                        self.data.fuel_lpg = True if 'LPG' in poi_data.get(
                            'services') else False
                        self.data.fuel_octane_95 = True
                        self.data.fuel_diesel = True
                        self.data.fuel_octane_100 = True
                        self.data.fuel_diesel_gtl = True
                        self.data.compressed_air = True
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
