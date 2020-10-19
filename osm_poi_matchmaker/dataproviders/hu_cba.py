# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        extract_javascript_variable, clean_opening_hours_2, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_cba(DataProvider):

    def constains(self):
        self.link = 'http://www.cba.hu/uzletlista'
        self.POI_COMMON_TAGS = ""
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [
            {'poi_code': 'hucbacon', 'poi_name': 'CBA', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'convenience', 'brand': 'CBA', " + POS_HU_GEN + PAY_CASH + "}",
             'poi_url_base': 'https://www.cba.hu', 'poi_search_name': '(cba abc|cba)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 5,
             'preserve_original_name': True},
            {'poi_code': 'hucbasup', 'poi_name': 'CBA', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'supermarket', 'brand': 'CBA',  'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.cba.hu', 'poi_search_name': '(cba abc|cba)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 5,
             'preserve_original_name': True},
            {'poi_code': 'huprimacon', 'poi_name': 'Príma', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'convenience', 'brand': 'Príma',  'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.prima.hu', 'poi_search_name': '(príma abc|prima abc|príma|prima)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 23,
             'preserve_original_name': True},
            {'poi_code': 'huprimasup', 'poi_name': 'Príma', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'supermarket', 'brand': 'Príma',  'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.prima.hu', 'poi_search_name': '(príma abc|prima abc|príma|prima)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 23,
             'preserve_original_name': True}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                text = json.loads(extract_javascript_variable(soup, 'boltok_nyers'))
                for poi_data in text:
                    # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                    self.data.city = clean_city(poi_data.get('A_VAROS'))
                    self.data.postcode = poi_data.get('A_IRSZ').strip()
                    self.data.branch = poi_data.get('P_NAME').strip()
                    self.data.name = 'Príma' if 'Príma' in self.data.branch else 'CBA'
                    self.data.code = 'huprimacon' if 'Príma' in self.data.branch else 'hucbacon'
                    for i in range(0, 7):
                        self.data.day_open(i, clean_opening_hours_2(
                            poi_data.get('PS_OPEN_FROM_{}'.format(i + 1))) if poi_data.get(
                            'PS_OPEN_FROM_{}'.format(
                                i + 1)) is not None else None)
                        self.data.day_close(i, clean_opening_hours_2(
                            poi_data.get('PS_OPEN_TO_{}'.format(i + 1))) if poi_data.get(
                            'PS_OPEN_TO_{}'.format(
                                i + 1)) is not None else None)
                    self.data.original = poi_data.get('A_CIM')
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('PS_GPS_COORDS_LAT'),
                                                                     poi_data.get('PS_GPS_COORDS_LNG'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data.get('A_CIM'))
                    if 'PS_PUBLIC_TEL' in poi_data and poi_data.get('PS_PUBLIC_TEL') != '':
                        self.data.phone = clean_phone_to_str(poi_data.get('PS_PUBLIC_TEL'))
                    else:
                        self.data.phone = None
                    if 'PS_PUBLIC_EMAIL' in poi_data and poi_data.get('PS_PUBLIC_EMAIL') != '':
                        self.data.email = poi_data.get('PS_PUBLIC_EMAIL')
                    else:
                        self.data.email = None
                    self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            logging.exception('Exception occurred')

            logging.error(e)
