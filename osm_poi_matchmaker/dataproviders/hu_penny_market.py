# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city, clean_javascript_variable
    from osm_poi_matchmaker.libs.geo import check_geom
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


class hu_penny_market():

    def __init__(self, session, link, download_cache, filename='hu_penny_market.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def types(self):
        data = [{'poi_code': 'hupennysup', 'poi_name': 'Penny Market',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Penny Market Kft.', 'brand': 'Penny Market', 'internet_access': 'wlan', 'internet_access:fee': 'no', 'internet_access:ssid': 'PENNY FREE WLAN', 'contact:email': 'ugyfelszolgalat@penny.hu', 'contact:facebook': 'https://www.facebook.com/PennyMarketMagyarorszag', 'contact:instagram': 'https://www.instagram.com/pennymarkethu/', 'contact:youtube': 'https://www.youtube.com/channel/UCSy0KKUrDxVWkx8qicky_pQ', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'ref:vatin:hu': '10969629-2-44'}",
                 'poi_url_base': 'https://www.penny.hu'}]
        return data

    def process(self):
        logging.warning('Not implemented. Skipping ...')

