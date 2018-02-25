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

POST_DATA = {'kepnelkul': 'true', 'latitude': '47.498', 'longitude': '19.0399', 'tipus': 'patika'}

class hu_kulcs_patika():

    def __init__(self, session, link, download_cache, filename='hu_kulcs_patika.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def types(self):
        data = [{'poi_code': 'hukulcspha', 'poi_name': 'Kulcs patika',
                 'poi_tags': "{'amenity': 'pharmacy', 'dispensing': 'yes', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.kulcspatika.hu'}]
        return data

    def process(self):
        '''
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), POST_DATA)
        insert_data = []
        if soup != None:

            text = json.loads(soup.get_text())
        '''
        with open(os.path.join(self.download_cache, self.filename), 'r') as f:
            insert_data = []
            text = json.load(f)
            for poi_data in text:
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['cim'])
                if 'Kulcs patika' not in poi_data['nev']:
                    name = poi_data['nev'].strip()
                    branch = None
                else:
                    name = 'Kulcs patika'
                    branch = poi_data['nev'].strip()
                code = 'hukulcspha'
                website = poi_data['link'].strip() if poi_data['link'] is not None else None
                city = clean_city(poi_data['helyseg'])
                postcode = poi_data['irsz'].strip()
                geom = check_geom(poi_data['marker_position']['latitude'], poi_data['marker_position']['longitude'])
                original = poi_data['cim']
                ref = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, geom])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
