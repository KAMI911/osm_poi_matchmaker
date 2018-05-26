# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city, clean_phone
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'https://locator.shell.hu/deliver_country_csv.csv?footprint=HU&site=cf&launch_country=HU&networks=ALL'


class hu_shell():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_shell.csv'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hushellfu', 'poi_name': 'Shell', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Shell', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'https://www.shell.hu'},
                {'poi_code': 'humobpefu', 'poi_name': 'Mobil Petrol', 'poi_type': 'fuel',
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
                if poi_data['Brand'] == 'Shell':
                    name = 'Shell'
                    code = 'hushellfu'
                elif poi_data['Brand'] == 'Mobilpetrol':
                    name = 'Mobil Petrol'
                    code = 'humobpefu'
                postcode = poi_data['Post code']
                steet_tmp = poi_data['Address'].lower().split()
                for i in range(0, len(steet_tmp)-2):
                    steet_tmp[i] = steet_tmp[i].capitalize()
                steet_tmp = ' '.join(steet_tmp)
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    steet_tmp)
                if poi_data['City'] != '':
                    city = clean_city(poi_data['City'].title())
                else:
                    if poi_data['Name'] != '':
                        city = clean_city(poi_data['Name'].title())
                    else:
                        city = None
                branch = poi_data['Name'].strip()
                website = None
                if poi_data['24 Hour'] == True:
                    nonstop = True
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
                else:
                    nonstop = False
                    mo_o = '06:00'
                    th_o = '06:00'
                    we_o = '06:00'
                    tu_o = '06:00'
                    fr_o = '06:00'
                    sa_o = '06:00'
                    su_o = '06:00'
                    mo_c = '22:00'
                    th_c = '22:00'
                    we_c = '22:00'
                    tu_c = '22:00'
                    fr_c = '22:00'
                    sa_c = '22:00'
                    su_c = '22:00'
                original = poi_data['Address']
                ref = None
                lat, lon = check_hu_boundary(poi_data['GPS Latitude'], poi_data['GPS Longitude'])
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                if 'Telephone' in poi_data and poi_data['Telephone'] != '':
                    phone = clean_phone(str(poi_data['Telephone']))
                else:
                    phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c, fr_c, sa_c, su_c])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
