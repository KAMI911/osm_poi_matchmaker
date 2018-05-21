# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_javascript_variable
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_COLS = poi_array_structure.POI_COLS
POI_DATA = ''


class hu_penny_market():

    def __init__(self, session, download_cache, filename='hu_penny_market.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hupennysup', 'poi_name': 'Penny Market', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Penny Market Kft.', 'brand': 'Penny Market', 'internet_access': 'wlan', 'internet_access:fee': 'no', 'internet_access:ssid': 'PENNY FREE WLAN', 'contact:email': 'ugyfelszolgalat@penny.hu', 'contact:facebook': 'https://www.facebook.com/PennyMarketMagyarorszag', 'contact:instagram': 'https://www.instagram.com/pennymarkethu/', 'contact:youtube': 'https://www.youtube.com/channel/UCSy0KKUrDxVWkx8qicky_pQ', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'ref:vatin:hu': '10969629-2-44'}",
                 'poi_url_base': 'https://www.penny.hu'}]
        nonstop = None
        mo_o = None
        th_o = None
        we_o = None
        tu_o = None
        fr_o = None
        sa_o = None
        su_o = None
        mo_c = None
        th_c = None
        we_c = None
        tu_c = None
        fr_c = None
        sa_c = None
        su_c = None
        return data

    def process(self):
        logging.warning('Not implemented. Skipping ...')

