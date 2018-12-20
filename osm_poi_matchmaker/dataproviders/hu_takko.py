# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external, query_osm_city_name_gpd
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_OTP
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_tesco(DataProvider):


    def constains(self):
        self.link = ''
        self.POI_COMMON_TAGS = "'operator': 'Takko Fashion Kft.', 'operator:addr': '2040 Budaörs, Ébner György köz 4.'," \
                               " 'ref:HU:company': '13-10-040628', 'ref:vatin:hu': '1335199-2-13', 'ref:vatin': 'HU1335199', 'brand': 'Takko'," \
                               " 'website': 'https://www.takko.com/hu-hu/'" \
                               " 'addr:country': 'HU', 'loyalty_card': 'yes'," + POS_OTP + " 'air_conditioning': 'yes'"
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hutakkocl', 'poi_name': 'Takko', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'clothes', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://takko.hu', 'poi_search_name': 'takko'},]
        return self.__types

    def process(self):
        pass