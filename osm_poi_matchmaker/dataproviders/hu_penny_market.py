# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_javascript_variable
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_DATA = ''


class hu_penny_market():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_penny_market.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hupennysup', 'poi_name': 'Penny Market', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Penny Market Kft.', 'brand': 'Penny Market', 'internet_access': 'wlan', 'internet_access:fee': 'no', 'internet_access:ssid': 'PENNY FREE WLAN', 'email': 'ugyfelszolgalat@penny.hu', 'facebook': 'https://www.facebook.com/PennyMarketMagyarorszag', 'instagram': 'https://www.instagram.com/pennymarkethu/', 'youtube': 'https://www.youtube.com/channel/UCSy0KKUrDxVWkx8qicky_pQ', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'ref:vatin:hu': '10969629-2-44'}",
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
        summer_mo_o = None
        summer_th_o = None
        summer_we_o = None
        summer_tu_o = None
        summer_fr_o = None
        summer_sa_o = None
        summer_su_o = None
        summer_mo_c = None
        summer_th_c = None
        summer_we_c = None
        summer_tu_c = None
        summer_fr_c = None
        summer_sa_c = None
        summer_su_c = None
        lunch_break_start = None
        lunck_break_stop = None
        opening_hours = None
        phone = None
        email = None
        return data

    def process(self):
        logging.warning('Not implemented. Skipping ...')
