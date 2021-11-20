# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    import os
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_budapest_bank(DataProvider):

    def constains(self):
        self.link = 'https://www.budapestbank.hu/apps/backend/branch-and-atm'
        self.tags = {'brand': 'Budapest Bank', 'brand:wikidata': 'Q27493463', 'bic': 'BUDAHUHB',
                     'brand:wikipedia': 'en:Budapest Bank', 'operator': 'Budapest Bank Zrt.',
                     'operator:addr': '1138 Budapest, Váci út 193.', 'ref:vatin': 'HU10196445',
                     'ref:vatin:hu': '10196445-4-44', 'ref:HU:company': '01 10 041037', 'air_conditioning': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hubpbank = {'amenity': 'bank', 'atm': 'yes',
                    'air_conditioning': 'yes', }
        hubpbank.update(self.tags)
        hubpatm = {'amenity': 'atm'}
        hubpatm.update(self.tags)
        self.__types = [
            {'poi_code': 'hubpbank', 'poi_name': 'Budapest Bank', 'poi_type': 'bank',
             'poi_tags': hubpbank, 'poi_url_base': 'https://www.budapestbank.hu',
             'poi_search_name': '(budapest bank|bp bank)',
             'poi_search_avoid_name': '(otpbank|otp|otp bank)',
             'osm_search_distance_perfect': 300, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'hubpatm', 'poi_name': 'Budapest Bank ATM', 'poi_type': 'atm',
             'poi_tags': hubpatm,
             'poi_url_base': 'https://www.budapestbank.hu',
             'poi_search_name': '(budapest bank|budapest bank atm|bp bank|bp bank atm)',
             'poi_search_avoid_name': '(otp atm|otp)',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    if poi_data['point_category'] == 1:
                        self.data.name = 'Budapest Bank'
                        self.data.code = 'hubpbank'
                        self.data.public_holiday_open = False
                    else:
                        self.data.name = 'Budapest Bank ATM'
                        self.data.code = 'hubpatm'
                        self.data.public_holiday_open = True
                    self.data.postcode = poi_data['point_zip']
                    self.data.city = poi_data['point_city']
                    self.data.lat, self.data.lon = check_hu_boundary(
                        poi_data['point_latitude'], poi_data['point_longitude'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(poi_data['point_street'])
                    self.data.original = poi_data['point_street']
                    self.data.branch = poi_data['point_name']
                    # Processing opening hours
                    oh = []
                    if poi_data.get('point_open_mon') is not None:
                      self.data_mo_o = poi_data.get('point_open_mon').split('-')[0] if poi_data.get('point_open_mon').split('-')[0] is not None else None
                      self.data_mo_c = poi_data.get('point_open_mon').split('-')[1] if poi_data.get('point_open_mon').split('-')[1] is not None else None

                    if poi_data.get('point_open_tue') is not None:
                      self.data_tu_o = poi_data.get('point_open_tue').split('-')[0] if poi_data.get('point_open_tue').split('-')[0] is not None else None
                      self.data_tu_c = poi_data.get('point_open_tue').split('-')[1] if poi_data.get('point_open_tue').split('-')[1] is not None else None

                    if poi_data.get('point_open_wed') is not None:
                      self.data_we_o = poi_data.get('point_open_wed').split('-')[0] if poi_data.get('point_open_wed').split('-')[0] is not None else None
                      self.data_we_c = poi_data.get('point_open_wed').split('-')[1] if poi_data.get('point_open_wed').split('-')[1] is not None else None

                    if poi_data.get('point_open_thu') is not None:
                      self.data_th_o = poi_data.get('point_open_thu').split('-')[0] if poi_data.get('point_open_thu').split('-')[0] is not None else None
                      self.data_th_c = poi_data.get('point_open_thu').split('-')[1] if poi_data.get('point_open_thu').split('-')[1] is not None else None

                    if poi_data.get('point_open_fri') is not None:
                      self.data_fr_o = poi_data.get('point_open_fri').split('-')[0] if poi_data.get('point_open_fri').split('-')[0] is not None else None
                      self.data_fr_c = poi_data.get('point_open_fri').split('-')[1] if poi_data.get('point_open_fri').split('-')[1] is not None else None

                    if poi_data.get('point_open_sat') is not None:
                      self.data_sa_o = poi_data.get('point_open_sat').split('-')[0] if poi_data.get('point_open_sat').split('-')[0] is not None else None
                      self.data_sa_c = poi_data.get('point_open_sat').split('-')[1] if poi_data.get('point_open_sat').split('-')[1] is not None else None

                    if poi_data.get('point_open_sun') is not None:
                      self.data_su_o = poi_data.get('point_open_sun').split('-')[0] if poi_data.get('point_open_sun').split('-')[0] is not None else None
                      self.data_su_c = poi_data.get('point_open_sun').split('-')[1] if poi_data.get('point_open_sun').split('-')[1] is not None else None
                    if self.data.code == 'hubpatm':
                        self.data.nonstop = True
                    else:
                        self.data.nonstop = False
                    self.data.add()
        except Exception as e:
            logging.exception('Exception occurred')
            logging.error(e)
