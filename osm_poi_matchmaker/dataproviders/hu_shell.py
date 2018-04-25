# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city, clean_javascript_variable
    from osm_poi_matchmaker.libs.geo import check_geom
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


class hu_shell():

    def __init__(self, session, link, download_cache, filename='hu_mol.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hushellfu', 'poi_name': 'Shell',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Shell', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'https://www.shell.hu'},
                {'poi_code': 'humobpefu', 'poi_name': 'Mobil Petrol',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Mobil Petrol', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'https://www.shell.hu'}
                ]
        return data

    def process(self):
        csv = save_downloaded_pd('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if csv is not None:
            csv[['Post code']] = csv[['Post code']].fillna('0000')
            csv[['Post code']] = csv[['Post code']].astype(int)
            csv[['Telephone']] = csv[['Telephone']].fillna('0')
            csv[['Telephone']] = csv[['Telephone']].astype(int)
            csv[['City']] = csv[['City']].fillna('')
            csv[['Name']] = csv[['Name']].fillna('')
            insert_data = []
            poi_dict = csv.to_dict('records')
            for poi_data in poi_dict:
                print(poi_data)
                if poi_data['Brand'] == 'Shell':
                    name = 'Shell'
                    code = 'hushellfu'
                elif poi_data['Brand'] == 'Mobilpetrol':
                    name = 'Mobil Petrol'
                    code = 'humobpefu'
                postcode = poi_data['Post code']
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['Address'].title())
                if poi_data['City'] != '':
                    print(poi_data['City'])
                    city = clean_city(poi_data['City'].title())
                else:
                    if poi_data['Name'] != '':
                        city = clean_city(poi_data['Name'].title())
                    else:
                        city = None
                branch = poi_data['Name'].strip()
                website = None
                original = poi_data['Address']
                ref = None
                geom = check_geom(poi_data['GPS Latitude'], poi_data['GPS Longitude'])
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, geom])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
