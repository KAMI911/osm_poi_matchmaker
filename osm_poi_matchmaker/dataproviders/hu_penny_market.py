# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from libs.address import clean_javascript_variable
    from libs.osm import query_postcode_osm_external
    from libs.poi_dataset import POIDataset
    from utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_penny_market(DataProvider):


    def constains(self):
        self.link = ''
        self.POI_COMMON_TAGS = ""

    def types(self):
        self.__types = [{'poi_code': 'hupennysup', 'poi_name': 'Penny Market', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Penny Market Kft.', 'brand': 'Penny Market', 'addr:country': 'HU', 'internet_access': 'wlan', 'internet_access:fee': 'no', 'internet_access:ssid': 'PENNY FREE WLAN', 'contact:email': 'ugyfelszolgalat@penny.hu', 'contact:facebook': 'https://www.facebook.com/PennyMarketMagyarorszag', 'contact:instagram': 'https://www.instagram.com/pennymarkethu', 'contact:youtube': 'https://www.youtube.com/channel/UCSy0KKUrDxVWkx8qicky_pQ', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'ref:vatin:hu': '10969629-2-44', 'ref:vatin': 'HU10969629'}",
                 'poi_url_base': 'https://www.penny.hu', 'poi_search_name': '(penny market|penny)'}]
        data = POIDataset()
        data.nonstop = None
        data.phone = None
        data.email = None
        data.public_holiday_open = False
        return self.__types

    def process(self):
        logging.warning('Not implemented. Skipping ...')
