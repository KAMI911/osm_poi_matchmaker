# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, PATTERN_FULL_URL
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_benu(DataProvider):


    def constains(self):
        self.link = 'https://benu.hu/wordpress-core/wp-admin/admin-ajax.php?action=asl_load_stores&nonce=1900018ba1&load_all=1&layout=1'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hubenupha', 'poi_name': 'Benu gyógyszertár', 'poi_type': 'pharmacy',
                 'poi_tags': "{'amenity': 'pharmacy', 'brand': 'Benu gyógyszertár', 'dispensing': 'yes', " + POS_HU_GEN + PAY_CASH + " 'contact:facebook':'https://www.facebook.com/BENUgyogyszertar', 'contact:youtube': 'https://www.youtube.com/channel/UCBLjL10QMtRHdkak0h9exqg', 'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://benu.hu', 'poi_search_name': '(benu gyogyszertár|benu)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 20, 'preserve_original_name': True}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                try:
                    text = json.loads(soup.get_text())
                    for poi_data in text:
                        if 'BENU Gyógyszertár' not in poi_data['title']:
                            self.data.name = poi_data['title'].strip()
                            self.data.branch = None
                        else:
                            self.data.name = 'Benu gyógyszertár'
                            self.data.branch = poi_data['title'].strip()
                        self.data.code = 'hubenupha'
                        if poi_data['description'] is not None:
                            pu_match = PATTERN_FULL_URL.match(poi_data['description'])
                            self.data.website = pu_match.group(0).strip() if pu_match is not None else None
                        else:
                            self.data.website = None
                        self.data.city = clean_city(poi_data['city'])
                        self.data.postcode = poi_data['postal_code'].strip()
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data['lat'], poi_data['lng'])
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            poi_data['street'])
                        self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                                    self.data.postcode)
                        self.data.original = poi_data['street']
                        if 'phone' in poi_data and poi_data['phone'] != '':
                            self.data.phone = clean_phone_to_str(poi_data['phone'])
                        else:
                            self.data.phone = None
                        self.data.public_holiday_open = False
                        self.data.add()
                except Exception as err:
                    logging.error(err)
                    logging.error(traceback.print_exc())
                if self.data.lenght() < 1:
                    logging.warning('Resultset is empty. Skipping ...')
                else:
                    insert_poi_dataframe(self.session, self.data.process())
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
