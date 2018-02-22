# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe, search_for_postcode
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_city_street_housenumber_address, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


PATTERN_TOM_MARKET = re.compile("title: '(.*)'")


class hu_tom_market():

    def __init__(self, session, link, download_cache, filename='hu_tom_market.html'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def types(self):
        data = [
            {'poi_code': 'hutommacon', 'poi_name': 'Tom Market',
             'poi_tags': "{'shop': 'convenience', 'brand': 'Tom Market', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.tommarket.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            poi_data = soup.find_all('script', text = re.compile('var\s*marker'))
            poi_data_match = PATTERN_TOM_MARKET.findall(str(poi_data))
            for poi_data in poi_data_match:
                    #if poi_data_match is not None else None
                if poi_data == None:
                    print('1')
                    print(str(poi_data))
                else:
                    print(poi_data)
                city, street, housenumber, conscriptionnumber = extract_city_street_housenumber_address(poi_data)
                city = clean_city(city)
                postcode = None
                if postcode is None:
                    postcode = search_for_postcode(self.session, city)
                name = 'Tom Market'
                code = 'hutommacon'
                branch = None
                website = None
                original = poi_data
                ref = None
                geom = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, geom])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
