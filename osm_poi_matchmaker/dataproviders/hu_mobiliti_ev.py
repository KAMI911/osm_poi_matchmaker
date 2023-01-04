# -*- coding: utf-8 -*-
import numpy as np

try:
    import logging
    import sys
    import os
    import pandas as pd
    import math
    import traceback
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_url, clean_string
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

    def contains(self):
        self.link = os.path.join(
            config.get_directory_cache_url(), 'hu_mobiliti_ev.csv')
        self.tags = {'amenity': 'charging_station', 'authentication:app': 'yes', 'authentication:none': 'yes',
                     'authentication:membership_card': 'yes', 'operator': 'NKM Mobilitás Kft.',
                     'operator:addr': '1081 Budapest, II. János Pál pápa tér 20.', 'fee': 'yes', 'parking:fee': 'no',
                     'opening_hours': '24/7', 'ref:vatin': 'HU23443486', 'ref:HU:vatin': '23443486-2-42',
                     'ref:HU:company': '01-09-965868', 'contact:website': 'https://www.mobiliti.hu/emobilitas',
                     'contact:email': 'help@mobiliti.hu', 'contact:phone': '+36 62 565 758', }
        self.filetype = FileType.csv
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [
            {'poi_code': 'humobilchs', 'poi_common_name': 'Mobiliti', 'poi_type': 'charging_station',
             'poi_tags': self.tags, 'poi_url_base': 'https://www.mobiliti.hu',
             'poi_search_name': '(mobility|e-mobi|emobi|e-töltőpont)', 'poi_search_avoid_name': '(tesla|supercharger|plugee)',
             'osm_search_distance_perfect': 50,
             'osm_search_distance_safe': 30, 'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def process(self):
        try:
            logging.info('Processing file: {}'.format(self.link))
            cvs = pd.read_csv(self.link, encoding='UTF-8', sep='\t', skiprows=0)
            logging.info(cvs)
            if cvs is not None:
                poi_dict = cvs.to_dict('records')
                logging.info(poi_dict)
                for poi_data in poi_dict:
                    logging.info(poi_data)
                    try:
                        self.data.code = 'humobilchs'
                        self.data.ref = clean_string(poi_data.get('Mobiliti azonosító'))
                        self.data.branch = clean_string(poi_data.get('Töltőpont neve'))
                        self.data.postcode = clean_string(poi_data.get('Irányító szám'))
                        self.data.city = clean_city(poi_data.get('Település'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(
                                poi_data.get('Cím'))
                        self.data.original = poi_data.get('Cím')
                        temp = poi_data.get('GPS koordináták')
                        if temp is None:
                            continue
                        else:
                            self.data.lat, self.data.lon = temp.split(',')
                        self.data.lat, self.data.lon = check_hu_boundary(
                            self.data.lat, self.data.lon)
                        if poi_data.get('Darab (CHAdeMO)') is not None and poi_data.get('Darab (CHAdeMO)') != '' and \
                                not math.isnan(poi_data.get('Teljesítmény (CHAdeMO)')):
                            self.data.socket_chademo = poi_data.get('Darab (CHAdeMO)')
                            self.data.socket_chademo_output = '{0:d} kW'.format(int(poi_data.get('Teljesítmény (CHAdeMO)')))
                        else:
                            self.data.socket_chademo = None
                            self.data.socket_chademo_output = None
                        if poi_data.get('Darab (CCS)') is not None and poi_data.get('Darab (CCS)') != '' and \
                                not math.isnan(poi_data.get('Teljesítmény (CCS)')):
                            self.data.socket_type2_combo = poi_data.get('Darab (CCS)')
                            self.data.socket_type2_combo_output = '{0:d} kW'.format(int(poi_data.get('Teljesítmény (CCS)')))
                        else:
                            self.data.socket_type2_combo = None
                            self.data.socket_type2_combo_output = None
                        if poi_data.get('Darab (Type 2)') is not None and poi_data.get('Darab (Type 2)') != '' and \
                                not math.isnan(poi_data.get('Teljesítmény (Type 2)')):
                            self.data.socket_type2_cable = poi_data.get('Darab (Type 2)')
                            self.data.socket_type2_cable_output = '{0:d} kW'.format(int(poi_data.get('Teljesítmény (Type 2)')))
                        else:
                            self.data.socket_type2_cable = None
                            self.data.socket_type2_cable_output = None
                        self.data.manufacturer = poi_data.get('Gyártó')
                        self.data.model = poi_data.get('Típus')
                        self.data.capacity = poi_data.get('Kapacitás')
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
