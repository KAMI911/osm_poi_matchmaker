# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

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
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Shell', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'https://www.shell.hu'},
                {'poi_code': 'humobpefu', 'poi_name': 'Mobil Petrol', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Mobil Petrol', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
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
            poi_dict = csv.to_dict('records')
            data = POIDataset()
            for poi_data in poi_dict:
                if poi_data['Brand'] == 'Shell':
                    data.name = 'Shell'
                    data.code = 'hushellfu'
                elif poi_data['Brand'] == 'Mobilpetrol':
                    data.name = 'Mobil Petrol'
                    data.code = 'humobpefu'
                data.postcode = poi_data['Post code']
                steet_tmp = poi_data['Address'].lower().split()
                for i in range(0, len(steet_tmp) - 2):
                    steet_tmp[i] = steet_tmp[i].capitalize()
                steet_tmp = ' '.join(steet_tmp)
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    steet_tmp)
                if poi_data['City'] != '':
                    data.city = clean_city(poi_data['City'].title())
                else:
                    if poi_data['Name'] != '':
                        data.city = clean_city(poi_data['Name'].title())
                    else:
                        data.city = None
                data.branch = poi_data['Name'].strip()
                if poi_data['24 Hour'] == True:
                    data.nonstop = True
                else:
                    data.nonstop = False
                    data.mo_o = '06:00'
                    data.tu_o = '06:00'
                    data.we_o = '06:00'
                    data.th_o = '06:00'
                    data.fr_o = '06:00'
                    data.sa_o = '06:00'
                    data.su_o = '06:00'
                    data.mo_c = '22:00'
                    data.tu_c = '22:00'
                    data.we_c = '22:00'
                    data.th_c = '22:00'
                    data.fr_c = '22:00'
                    data.sa_c = '22:00'
                    data.su_c = '22:00'
                data.original = poi_data['Address']
                data.lat, data.lon = check_hu_boundary(poi_data['GPS Latitude'], poi_data['GPS Longitude'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                if 'Telephone' in poi_data and poi_data['Telephone'] != '':
                    data.phone = clean_phone(str(poi_data['Telephone']))
                else:
                    data.phone = None
                data.email = None
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
