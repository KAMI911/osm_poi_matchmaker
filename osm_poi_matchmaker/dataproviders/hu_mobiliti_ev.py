# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import pandas as pd
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_mobiliti_ev(DataProvider):

    def constains(self):
        self.link = os.path.join(config.get_directory_cache_url(), 'hu_mobiliti_ev.csv')
        self.tags = {'amenity': 'charging_station', 'authentication:app': 'yes', 'authentication:none': 'yes',
                     'authentication:membership_card': 'yes', 'operator': 'NKM Mobilitás Kft.',
                     'operator:addr': '1081 Budapest, II. János Pál pápa tér 20.', 'fee': 'yes', 'parking:fee': 'no',
                     'opening_hours': '24/7', 'ref:vatin': 'HU23443486', 'ref:vatin:hu': '23443486-2-42',
                     'ref:HU:company': '01-09-965868', 'contact:website': 'https://www.mobiliti.hu/emobilitas',
                     'contact:email': 'help@mobiliti.hu', 'contact:phone': '+36 62 565 758', }
        self.filetype = FileType.csv
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [
            {'poi_code': 'humobilchs', 'poi_name': 'Mobiliti', 'poi_type': 'charging_station',
             'poi_tags': self.tags, 'poi_url_base': 'https://www.mobiliti.hu',
             'poi_search_name': '(mobility)', 'osm_search_distance_perfect': 50,
             'osm_search_distance_safe': 30, 'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def process(self):
        try:
            csv = pd.read_csv(self.link, encoding='UTF-8', sep=';', skiprows=1)
            if csv is not None:
                poi_dict = csv.to_dict('records')
                for poi_data in poi_dict:
                    self.data.name = 'Mobiliti'
                    self.data.code = 'humobilchs'
                    self.data.ref = poi_data.get('Mobiliti azonosító')
                    self.data.branch = poi_data.get('Töltőpont neve')
                    self.data.postcode = poi_data.get('Irányító szám')
                    self.data.city = clean_city(poi_data.get('Település'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(poi_data.get('Cím'))
                    self.data.original = poi_data.get('Cím')
                    temp = poi_data.get('GPS koordináták')
                    if temp is None:
                        continue
                    else:
                        self.data.lat, self.data.lon = temp.split(',')
                    self.data.lat, self.data.lon = check_hu_boundary(self.data.lat, self.data.lon)
                    self.data.socket_chademo = poi_data.get('Darab (CHAdeMO)')
                    self.data.socket_chademo_output = poi_data.get('Teljesítmény (CHAdeMO)')
                    self.data.socket_type2_combo = poi_data.get('Darab (CCS)')
                    self.data.socket_type2_combo_output = poi_data.get('Teljesítmény (CCS)')
                    self.data.socket_type2_cable = poi_data.get('Darab (Type 2)')
                    self.data.socket_type2_cable_output = poi_data.get('Teljesítmény (Type 2)')
                    self.data.manufacturer = poi_data.get('Gyártó')
                    self.data.model = poi_data.get('Típus')
                    self.data.capacity = poi_data.get('Kapacitás')
                    self.data.add()
        except Exception as e:
            logging.exception('Exception occurred')

            logging.error(e)
