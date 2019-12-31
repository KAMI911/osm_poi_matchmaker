# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from sys import exit
    import os
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external, query_osm_city_name_gpd
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


class hu_takko(DataProvider):


    def constains(self):
        self.link = ''
        self.POI_COMMON_TAGS = "'operator': 'Takko Fashion Kft.', " \
                               "'operator:addr': '2040 Budaörs, Ébner György köz 4.', " \
                               "'ref:HU:company': '13-10-040628', 'ref:vatin:hu': '1335199-2-13', " \
                               "'ref:vatin': 'HU1335199', 'brand': 'Takko', " \
                               "'contact:website': 'https://www.takko.com/hu-hu/', " \
                               " 'loyalty_card': 'yes', " + POS_HU_GEN + PAY_CASH + " 'air_conditioning': 'yes'"
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hutakkocl', 'poi_name': 'Takko', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'clothes', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://takko.hu', 'poi_search_name': 'takko'},]
        return self.__types

    def process(self):
        pass
