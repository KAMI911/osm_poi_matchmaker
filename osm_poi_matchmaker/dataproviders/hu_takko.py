# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_takko(DataProvider):

    def contains(self):
        self.link = ''
        self.tags = {'shop': 'clothes', 'operator': 'Takko Fashion Kft.',
                     'operator:addr': '2040 Budaörs, Ébner György köz 4.',
                     'ref:HU:company': '13-10-040628', 'ref:vatin:hu': '1335199-2-13', 'ref:vatin': 'HU1335199',
                     'brand': 'Takko', 'contact:website': 'https://www.takko.com/hu-hu/', 'loyalty_card': 'yes',
                     'air_conditioning': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hutakkocl = self.tags.copy()
        hutakkocl.update(POS_HU_GEN)
        hutakkocl.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hutakkocl', 'poi_name': 'Takko', 'poi_type': 'shop',
             'poi_tags': hutakkocl, 'poi_url_base': 'https://takko.hu', 'poi_search_name': 'takko'},
        ]
        return self.__types

    def process(self):
        pass
