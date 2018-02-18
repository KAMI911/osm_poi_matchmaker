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
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_city, clean_javascript_variable
    from osm_poi_matchmaker.libs.geo import check_geom
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


class hu_cib_bank():

    def __init__(self, session, link, download_cache, name):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.name = name

    def types(self):
        data = [{'poi_code': 'hucibbank', 'poi_name': 'CIB bank',
                 'poi_tags': "{'amenity': 'bank', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.', bic': 'CIBHHUHB', 'atm': 'yes'}",
                 'poi_url_base': 'https://www.cib.hu'},
                {'poi_code': 'hucibatm', 'poi_name': 'CIB',
                 'poi_tags': "{'amenity': 'atm', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.'}",
                 'poi_url_base': 'https://www.cib.hu'}]
        return data

    def process(self):
        if self.link:
            with open(self.link, 'r') as f:
                insert_data = []
                text = json.load(f)
                for poi_data in text['results']:
                    first_element = next(iter(poi_data))
                    if self.name == 'CIB bank':
                        name = 'CIB bank'
                        code = 'hucibbank'
                    else:
                        name = 'CIB'
                        code = 'hucibatm'
                    postcode, city, street, housenumber, conscriptionnumber = extract_all_address(
                        poi_data[first_element]['address'])
                    branch = None,
                    website = None,
                    geom = check_geom(poi_data[first_element]['latitude'], poi_data[first_element]['longitude'])
                    original = poi_data[first_element]['address']
                    ref = None
                    insert_data.append(
                        [code, postcode, city, name, branch, website, original, street, housenumber,
                         conscriptionnumber,
                         ref, geom])
                if len(insert_data) < 1:
                    logging.warning('Resultset is empty. Skipping ...')
                else:
                    df = pd.DataFrame(insert_data)
                    df.columns = POI_COLS
                    insert_poi_dataframe(self.session, df)
