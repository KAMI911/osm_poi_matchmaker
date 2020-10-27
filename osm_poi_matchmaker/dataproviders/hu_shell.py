# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_shell(DataProvider):

    def constains(self):
        self.link = 'https://locator.shell.hu/deliver_country_csv.csv?footprint=HU&site=cf&launch_country=HU&networks=ALL'
        self.tags = {'amenity': 'fuel', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.csv
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hushellfu = self.tags.copy()
        hushellfu.update({'brand': 'Shell', 'contact:phone': '+36 1 480 1114',
                          'contact:fax': '+36 1 999 8673', 'contact:website': 'https://shell.hu/',
                          'contact:facebook': 'https://www.facebook.com/ShellMagyarorszag/', 'contact:twitter': 'shell',
                          'brand:wikidata': 'Q154950', 'brand:wikipedia': 'hu:Royal Dutch Shell',
                          'air_conditioning': 'yes'})
        humobpefu = self.tags.copy()
        humobpefu.update({'brand': 'Mobil Petrol', 'contact:email': 'info@mpetrol.hu',
                          'contact:facebook': 'https://www.facebook.com/mpetrolofficial/', 'name': 'Mobil Petrol',
                          'operator:addr': '1095 Budapest, Ipar utca 2.', 'operator': 'MPH Power Zrt.'})
        self.__types = [
            {'poi_code': 'hushellfu', 'poi_name': 'Shell', 'poi_type': 'fuel', 'poi_tags': hushellfu,
             'poi_url_base': 'https://shell.hu', 'poi_search_name': 'shell',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 60},
            # {'poi_code': 'humobpefu', 'poi_name': 'Mobil Petrol', 'poi_type': 'fuel', 'poi_tags': humobpefu,
            #  'poi_url_base': 'http://mpetrol.hu/', 'poi_search_name': '(m petrol|m. petrol|mobil metrol|shell)',
            #  'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 60},
        ]
        return self.__types

    def process(self):
        try:
            csv = save_downloaded_pd('{}'.format(self.link), os.path.join(
                self.download_cache, self.filename))
            if csv is not None:
                csv[['Post code']] = csv[['Post code']].fillna('0000')
                csv[['Post code']] = csv[['Post code']].astype(int)
                csv[['Telephone']] = csv[['Telephone']].fillna('0')
                csv[['Telephone']] = csv[['Telephone']].astype(int)
                csv[['City']] = csv[['City']].fillna('')
                csv[['Name']] = csv[['Name']].fillna('')
                poi_dict = csv.to_dict('records')
                for poi_data in poi_dict:
                    if poi_data['Brand'] == 'Shell':
                        self.data.name = 'Shell'
                        self.data.code = 'hushellfu'
                        self.data.website = 'https://shell.hu/'
                    elif poi_data['Brand'] == 'Mobilpetrol':
                        # It seems Mobil Petrol data is outdated so do not process here
                        continue
                        """
                        self.data.name = 'M. Petrol'
                        self.data.code = 'humobpefu'
                        self.data.website = 'http://mpetrol.hu/'
                        """
                    self.data.postcode = poi_data.get(
                        'Post code') if poi_data.get('Post code') != '' else None
                    street_tmp = poi_data['Address'].lower().split()
                    for i in range(0, len(street_tmp) - 2):
                        street_tmp[i] = street_tmp[i].capitalize()
                    street_tmp = ' '.join(street_tmp)
                    if poi_data['City'] != '':
                        self.data.city = clean_city(poi_data['City'].title())
                    else:
                        if poi_data['Name'] != '':
                            self.data.city = clean_city(
                                poi_data['Name'].title())
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
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['GPS Latitude'],
                                                                     poi_data['GPS Longitude'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        street_tmp)
                    if 'Telephone' in poi_data and poi_data['Telephone'] != '':
                        self.data.phone = clean_phone_to_str(
                            str(poi_data['Telephone']))
                    else:
                        self.data.phone = None
                    self.data.email = None
                    self.data.add()
        except Exception as e:
            logging.exception('Exception occurred')

            logging.error(e)
