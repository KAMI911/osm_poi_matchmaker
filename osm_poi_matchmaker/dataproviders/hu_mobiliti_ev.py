# -*- coding: utf-8 -*-
import numpy as np

try:
    import logging
    import sys
    import os
    import math
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
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
        self.link = 'https://api.mobiliti.hu/ocpi-location/api/v1/own-and-foreign-locations'
        self.tags = {'amenity': 'charging_station', 'authentication:app': 'yes', 'authentication:none': 'yes',
                     'authentication:membership_card': 'yes', 'operator': 'NKM Mobilitás Kft.',
                     'operator:addr': '1081 Budapest, II. János Pál pápa tér 20.', 'fee': 'yes', 'parking:fee': 'no',
                     'opening_hours': '24/7', 'ref:vatin': 'HU23443486', 'ref:HU:vatin': '23443486-2-42',
                     'ref:HU:company': '01-09-965868', 'contact:website': 'https://www.mobiliti.hu/emobilitas',
                     'contact:email': 'help@mobiliti.hu', 'contact:phone': '+36 62 565 758', }
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [
            {'poi_code': 'humobilchs', 'poi_common_name': 'Mobiliti', 'poi_type': 'charging_station',
             'poi_tags': self.tags, 'poi_url_base': 'https://www.mobiliti.hu',
             'poi_search_name': '(mobility|e-mobi|emobi|e-töltőpont)', 'poi_search_avoid_name': '(tesla|supercharger|plugee)',
             'osm_search_distance_perfect': 400,
             'osm_search_distance_safe': 150, 'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def process(self):
        try:
            logging.info('Processing file: {}'.format(self.link))
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    try:
                        self.data.code = 'humobilchs'
                        try:
                            self.data.ref = clean_string(poi_data.get('id'))
                        except Exception as e:
                            logging.exception('Exception occurred: {}'.format(e))
                            logging.exception(traceback.format_exc())
                        self.data.branch = clean_string(poi_data.get('name'))
                        self.data.postcode = clean_string(poi_data.get('postalCode'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(
                                poi_data.get('Cím'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(poi_data.get('address'))
                        self.data.original = poi_data.get('address')
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('latitude'), poi_data.get('longitude'))
                        self.data.socket_type2_cable = None
                        self.data.socket_type2_cable_output = None
                        self.data.socket_type2_cable_current = None
                        self.data.socket_type2_cable_coltage = None
                        
                        self.data.socket_type2_cableless = None
                        self.data.socket_type2_cableless_output = None
                        self.data.socket_type2_cableless_current =None
                        self.data.socket_type2_cableless_voltage = None

                        self.data.socket_type2_combo = None
                        self.data.socket_type2_combo_output = None
                        self.data.socket_type2_combo_current = None
                        self.data.socket_type2_combo_voltage = None

                        self.data.socket_chademo = None
                        self.data.socket_chademo_output = None
                        self.data.socket_chademo_current = None
                        self.data.socket_chademo_voltage = None
                        for port in poi_data.get('evses'):
                            if port.get('plugType').upper() == 'TYPE2':
                                if port.get('cableAttached'):
                                    if self.data.socket_type2_cable is None:
                                        self.data.socket_type2_cable = port.get('quantity')
                                    else:
                                        self.data.socket_type2_cable = self.data.socket_type2_cable + port.get('quantity')
                                    self.data.socket_type2_cable_output = '{0:d} kW'.format(int(port.get('power')) // 1000)
                                    self.data.socket_type2_cable_current = int(port.get('current'))
                                    self.data.socket_type2_cable_voltage = int(port.get('voltage'))
                                else:
                                    if self.data.socket_type2_cableless is None:
                                        self.data.socket_type2_cableless = port.get('quantity')
                                    else:
                                        self.data.socket_type2_cableless = self.data.socket_type2_cableless + port.get('quantity')
                                    self.data.socket_type2_cableless_output = '{0:d} kW'.format(int(port.get('power')) // 1000)
                                    self.data.socket_type2_cableless_current = int(port.get('current'))
                                    self.data.socket_type2_cableless_voltage = int(port.get('voltage'))
                            elif port.get('plugType').upper() == 'CHADEMO':
                                if self.data.socket_chademo is None:
                                    self.data.socket_chademo = port.get('quantity')
                                else:
                                    self.data.socket_chademo = self.data.socket_chademo + port.get('quantity')
                                self.data.socket_chademo_output = '{0:d} kW'.format(int(port.get('power')) // 1000)
                                self.data.socket_chademo_current = int(port.get('current'))
                                self.data.socket_chademo_voltage = int(port.get('voltage'))
                            elif port.get('plugType').upper() == 'CCS':
                                if self.data.socket_type2_combo is None:
                                    self.data.socket_type2_combo = port.get('quantity')
                                else:
                                    self.data.socket_type2_combo = self.data.socket_type2_combo + port.get('quantity')
                                self.data.socket_type2_combo_output = '{0:d} kW'.format(int(port.get('power')) // 1000)
                                self.data.socket_type2_combo_current = int(port.get('current'))
                                self.data.socket_type2_combo_voltage = int(port.get('voltage'))
                            else:
                                logging.debug('Non processed EV port type.')
                        self.data.manufacturer = poi_data.get('manufacturer')
                        #self.data.model = poi_data.get('Típus')
                        #self.data.capacity = 0
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
