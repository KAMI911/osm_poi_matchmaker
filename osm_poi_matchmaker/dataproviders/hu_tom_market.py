# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    from dao.data_handlers import insert_poi_dataframe, search_for_postcode
    from libs.soup import save_downloaded_soup
    from libs.address import extract_city_street_housenumber_address, clean_city
    from libs.osm import query_postcode_osm_external
    from libs.poi_dataset import POIDataset
    from utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)

PATTERN_TOM_MARKET = re.compile("title: '(.*)'")


class hu_tom_market(DataProvider):


    def constains(self):
        self.link =  'http://tommarket.hu/shops'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'html'

    def types(self):
        self.__types = [
            {'poi_code': 'hutommacon', 'poi_name': 'Tom Market', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'convenience', 'brand': 'Tom Market',  'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.tommarket.hu', 'poi_search_name': 'tom market'}]
        return self.__types

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            poi_data = soup.find_all('script', text=re.compile('var\s*marker'))
            poi_data_match = PATTERN_TOM_MARKET.findall(str(poi_data))
            for poi_data in poi_data_match:
                # if poi_data_match is not None else None
                self.data.city, self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_city_street_housenumber_address(
                    poi_data)
                self.data.city = clean_city(self.data.city)
                self.data.postcode = None
                if self.data.postcode is None:
                    self.data.postcode = search_for_postcode(self.session, self.data.city)
                self.data.name = 'Tom Market'
                self.data.code = 'hutommacon'
                self.data.branch = None
                self.data.website = None
                self.data.original = poi_data
                self.data.ref = None
                self.data.geom = None
                self.data.phone = None
                self.data.email = None
                self.data.add()
