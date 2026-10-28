# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# Whole-world bounding box - OTP's own map widget sends the box currently on screen, but
# the endpoint just returns everything inside it and doesn't seem to cap or paginate the
# result, so requesting the entire world in one call returns the complete HU dataset too.
WORLD_BBOX = {'enclosingRectangle': [{'latitude': 90, 'longitude': 180}, {'latitude': -90, 'longitude': -180}]}

AVOID_NAME = '(cib|k&h|kh bank|raiffeisen|unicredit|erste|mbh|mkb|takarékbank|budapest bank|gránit|sberbank|magnet|oberbank|cetelem)'


class hu_otp_bank(DataProvider):
    """Imports OTP Bank branch/ATM locations in Hungary from the same JSON API
    (www.otpbank.hu/apps2/branch-atm-locator/{atm,branch}/list) OTP's own branch/ATM
    locator map uses."""

    def contains(self):
        self.branch_link = 'https://www.otpbank.hu/apps2/branch-atm-locator/branch/list'
        self.atm_link = 'https://www.otpbank.hu/apps2/branch-atm-locator/atm/list'
        self.link = self.branch_link
        self.tags = {'brand': 'OTP Bank', 'brand:wikidata': 'Q912778', 'operator': 'OTP Bank',
                     'operator:wikidata': 'Q912778', 'contact:website': 'https://www.otpbank.hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huotpbank = {'amenity': 'bank', 'atm': 'yes'}
        huotpbank.update(self.tags)
        huotpatm = {'amenity': 'atm'}
        huotpatm.update(self.tags)
        self.__types = [
            {'poi_code': 'huotpbank', 'poi_common_name': 'OTP Bank', 'poi_type': 'bank', 'poi_tags': huotpbank,
             'poi_url_base': 'https://www.otpbank.hu', 'poi_search_name': '(otp bank|otp)',
             'poi_search_avoid_name': AVOID_NAME, 'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'huotpatm', 'poi_common_name': 'OTP Bank ATM', 'poi_type': 'atm', 'poi_tags': huotpatm,
             'poi_url_base': 'https://www.otpbank.hu', 'poi_search_name': '(otp bank|otp)',
             'poi_search_avoid_name': AVOID_NAME, 'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def __process_list(self, link, cache_name, poi_code, is_atm):
        soup = save_downloaded_soup(link, os.path.join(self.download_cache, cache_name),
                                    self.filetype, False, post_data=WORLD_BBOX, json_body=True)
        if soup is None:
            return
        for poi_data in json.loads(soup):
            try:
                self.data.code = poi_code
                self.data.ref = clean_string(poi_data.get('id'))
                address = poi_data.get('address')
                self.data.original = clean_string(address)
                self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                    self.data.conscriptionnumber = extract_all_address_waxeye(address)
                self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('latitude'), poi_data.get('longitude'))
                if is_atm:
                    self.data.nonstop = poi_data.get('openingHours') == 'AVAILABLE_7_24'
                    self.data.public_holiday_open = True
                else:
                    self.data.public_holiday_open = False
                self.data.add()
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
                logging.exception(poi_data)

    def process(self):
        try:
            self.__process_list(self.branch_link, 'hu_otp_branch.json', 'huotpbank', False)
            self.__process_list(self.atm_link, 'hu_otp_atm.json', 'huotpatm', True)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
