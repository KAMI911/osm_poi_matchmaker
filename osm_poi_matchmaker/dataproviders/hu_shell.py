# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_shell(DataProvider):


    def constains(self):
        self.link = 'https://locator.shell.hu/deliver_country_csv.csv?footprint=HU&site=cf&launch_country=HU&networks=ALL'
        self.POI_COMMON_TAGS = ""

    def types(self):
        self.__types = [{'poi_code': 'hushellfu', 'poi_name': 'Shell', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Shell', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes', 'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.shell.hu', 'poi_search_name': 'shell'},
                {'poi_code': 'humobpefu', 'poi_name': 'Mobil Petrol', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Mobil Petrol', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'https://www.shell.hu', 'poi_search_name': '(mobil metrol|shell)'}
                ]
        return self.__types


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
            self.data = POIDataset()
            for poi_data in poi_dict:
                if poi_data['Brand'] == 'Shell':
                    self.data.name = 'Shell'
                    self.data.code = 'hushellfu'
                elif poi_data['Brand'] == 'Mobilpetrol':
                    self.data.name = 'Mobil Petrol'
                    self.data.code = 'humobpefu'
                self.data.postcode = poi_data['Post code']
                steet_tmp = poi_data['Address'].lower().split()
                for i in range(0, len(steet_tmp) - 2):
                    steet_tmp[i] = steet_tmp[i].capitalize()
                steet_tmp = ' '.join(steet_tmp)
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                    steet_tmp)
                if poi_data['City'] != '':
                    self.data.city = clean_city(poi_data['City'].title())
                else:
                    if poi_data['Name'] != '':
                        self.data.city = clean_city(poi_data['Name'].title())
                    else:
                        self.data.city = None
                self.data.branch = poi_data['Name'].strip()
                if poi_data['24 Hour'] == True:
                    self.data.nonstop = True
                    self.data.public_holiday_open = True
                else:
                    self.data.nonstop = False
                    self.data.mo_o = '06:00'
                    self.data.tu_o = '06:00'
                    self.data.we_o = '06:00'
                    self.data.th_o = '06:00'
                    self.data.fr_o = '06:00'
                    self.data.sa_o = '06:00'
                    self.data.su_o = '06:00'
                    self.data.mo_c = '22:00'
                    self.data.tu_c = '22:00'
                    self.data.we_c = '22:00'
                    self.data.th_c = '22:00'
                    self.data.fr_c = '22:00'
                    self.data.sa_c = '22:00'
                    self.data.su_c = '22:00'
                    self.data.public_holiday_open = False
                self.data.original = poi_data['Address']
                self.data.lat, self.data.lon = check_hu_boundary(poi_data['GPS Latitude'], poi_data['GPS Longitude'])
                self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                            self.data.postcode)
                if 'Telephone' in poi_data and poi_data['Telephone'] != '':
                    self.data.phone = clean_phone(str(poi_data['Telephone']))
                else:
                    self.data.phone = None
                self.data.email = None
                self.data.add()
