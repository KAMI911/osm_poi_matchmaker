# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    from dao.data_handlers import insert_poi_dataframe
    from libs.soup import save_downloaded_soup
    from libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_javascript_variable, clean_opening_hours_2, clean_phone_to_str
    from libs.geo import check_hu_boundary
    from libs.osm import query_postcode_osm_external
    from libs.poi_dataset import POIDataset
    from utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_cba(DataProvider):


    def constains(self):
        self.link = 'http://www.cba.hu/uzletlista'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'html'

    def types(self):
        self.__types = [
            {'poi_code': 'hucbacon', 'poi_name': 'CBA', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'convenience', 'brand': 'CBA', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.cba.hu', 'poi_search_name': '(cba abc|cba)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 60, 'osm_search_distance_unsafe': 5, 'preserve_original_name': True},
            {'poi_code': 'hucbasup', 'poi_name': 'CBA', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'supermarket', 'brand': 'CBA', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.cba.hu', 'poi_search_name': '(cba abc|cba)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 60, 'osm_search_distance_unsafe': 5, 'preserve_original_name': True},
            {'poi_code': 'huprimacon', 'poi_name': 'Príma', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'convenience', 'brand': 'Príma', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.prima.hu', 'poi_search_name': '(príma abc|prima abc|príma|prima)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 50, 'osm_search_distance_unsafe': 23, 'preserve_original_name': True},
            {'poi_code': 'huprimasup', 'poi_name': 'Príma', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'supermarket', 'brand': 'Príma', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.prima.hu', 'poi_search_name': '(príma abc|prima abc|príma|prima)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 50, 'osm_search_distance_unsafe': 23, 'preserve_original_name': True}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup != None:
                # parse the html using beautiful soap and store in variable `soup`
                pattern = re.compile('^\s*var\s*boltok_nyers.*')
                script = soup.find('script', text=pattern)
                m = pattern.match(script.get_text())
                data = m.group(0)
                data = clean_javascript_variable(data, 'boltok_nyers')
                text = json.loads(data)
                for poi_data in text:
                    # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                    self.data.city = clean_city(poi_data['A_VAROS'])
                    self.data.postcode = poi_data['A_IRSZ'].strip()
                    self.data.branch = poi_data['P_NAME'].strip()
                    self.data.name = 'Príma' if 'Príma' in self.data.branch else 'CBA'
                    self.data.code = 'huprimacon' if 'Príma' in self.data.branch else 'hucbacon'
                    for i in range(0, 7):
                        self.data.day_open(i, clean_opening_hours_2(poi_data['PS_OPEN_FROM_{}'.format(i + 1)]) if poi_data[
                                                                                                                 'PS_OPEN_FROM_{}'.format(
                                                                                                                     i + 1)] is not None else None)
                        self.data.day_close(i, clean_opening_hours_2(poi_data['PS_OPEN_TO_{}'.format(i + 1)]) if poi_data[
                                                                                                                'PS_OPEN_TO_{}'.format(
                                                                                                                    i + 1)] is not None else None)
                    self.data.original = poi_data['A_CIM']
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['PS_GPS_COORDS_LAT'], poi_data['PS_GPS_COORDS_LNG'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data['A_CIM'])
                    self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                                self.data.postcode)
                    if 'PS_PUBLIC_TEL' in poi_data and poi_data['PS_PUBLIC_TEL'] != '':
                        self.data.phone = clean_phone_to_str(poi_data['PS_PUBLIC_TEL'])
                    else:
                        self.data.phone = None
                    if 'PS_PUBLIC_EMAIL' in poi_data and poi_data['PS_PUBLIC_EMAIL'] != '':
                        self.data.email = poi_data['PS_PUBLIC_EMAIL']
                    else:
                        self.data.email = None
                    self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            traceback.print_exc()
            logging.error(e)
