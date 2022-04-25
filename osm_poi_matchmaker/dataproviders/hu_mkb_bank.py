# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    import os
    import traceback
    import numpy as np
    import pandas as pd
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_email, replace_html_newlines,\
        extract_phone_number
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_mkb_bank(DataProvider):

    def constains(self):
        self.link = os.path.join(
            config.get_directory_cache_url(), 'hu_mkb_bank.csv')
        self.tags = {'brand': 'MKB Bank', 'brand:wikidata': 'Q916185', 'bic': 'MKKBHUHB',
                     'brand:wikipedia': 'hu:MKB Bank', 'operator': 'MKB Bank Nyrt.',
                     'operator:addr': '1056 Budapest, Váci u. 38.', 'ref:vatin': 'HU10011922',
                     'ref:vatin:hu': '10011922-4-44', 'ref:HU:company': '01 10 040952', 'air_conditioning': 'yes'}
        self.filetype = FileType.csv
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        humkbbank = {'amenity': 'bank', 'atm': 'yes',
                     'air_conditioning': 'yes', }
        humkbbank.update(self.tags)
        humkbatm = {'amenity': 'atm'}
        humkbatm.update(self.tags)
        self.__types = [
            {'poi_code': 'humkbbank', 'poi_name': 'MKB Bank', 'poi_type': 'bank',
             'poi_tags': humkbbank, 'poi_url_base': 'https://www.mkb.hu',
             'poi_search_name': '(mkb bank)',
             'poi_search_avoid_name': '(otpbank|otp|otp bank|raiffeisenbank|raiffeisen bank|kh bank|k&h|raiffeisen|budapest bank|takarék bank|takarék)',
             'osm_search_distance_perfect': 300, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'humkbatm', 'poi_name': 'MKB Bank ATM', 'poi_type': 'atm',
             'poi_tags': humkbatm,
             'poi_url_base': 'https://www.mkb.hu',
             'poi_search_name': '(mkb bank|mkb bank atm)',
             'poi_search_avoid_name': '(otp atm|otp)',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def process(self):
        try:
            logging.info('Processing file: {}'.format(self.link))
            cvs = pd.read_csv(self.link, encoding='UTF-8', sep='\t', skiprows=0)
            logging.info(cvs)
            if cvs is not None:
                poi_dict = cvs.to_dict('records')
                logging.info(poi_dict)
                for poi_data in poi_dict:
                    logging.info(poi_dict)
                    try:
                        if poi_data.get('Típus') == 'FIOKATM':
                            self.data.name = 'MKB Bank'
                            self.data.code = 'humkbbank'
                            self.data.public_holiday_open = False
                        else:
                            self.data.name = 'MKB Bank ATM'
                            self.data.code = 'humkbatm'
                            self.data.public_holiday_open = True
                        self.data.postcode = poi_data.get('Körzetszám')
                        self.data.city = poi_data.get('Város')
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('Földrajzi szélesség').replace(',','.'),
                            poi_data.get('Földrajzi hosszúság').replace(',','.'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(poi_data['Cím'])
                        self.data.original = poi_data.get('Cím')
                        self.data.email = clean_email(poi_data.get('E-mail cím'))
                        if poi_data.get('Időpontfoglalás URL') is not None and poi_data.get(
                                'Időpontfoglalás URL') != '':
                            self.data.website = poi_data.get('Időpontfoglalás URL')
                        else:
                            self.data.website = None
                        self.data.ref = poi_data.get('ATM / Fiók azonosítója')
                        self.data.description = poi_data.get('Megjegyzés')
                        self.data.description = replace_html_newlines(self.data.description)
                        if 'Akadálymentesen' in self.data.description:
                            logging.debug('TODO: Implement wheelchair field')
                        self.data.phone = extract_phone_number(self.data.description)
                        if self.data.code == 'humkbatm':
                            self.data.nonstop = True
                        else:
                            self.data.nonstop = False
                            # Processing opening hours
                            self.data.opening_hours_table = []
                            if poi_data.get('Hétfő nyitás') is not None and str(poi_data.get('Hétfő nyitás')).strip() != '':
                                self.data.mo_o = str(poi_data.get('Hétfő nyitás')).strip() if str(poi_data.get(
                                    'Hétfő nyitás')).strip() is not None else None
                            if poi_data.get('Hétfő nyitás') is not None and str(poi_data.get('Hétfő zárás')).strip() != '':
                                self.data.mo_c = str(poi_data.get('Hétfő zárás')).strip() if str(poi_data.get(
                                    'Hétfő zárás')).strip() is not None else None

                            if poi_data.get('Kedd nyitás') is not None and str(poi_data.get('Kedd nyitás')).strip() != '':
                                self.data.tu_o = str(poi_data.get('Kedd nyitás')).strip() if str(poi_data.get(
                                    'Kedd nyitás')).strip() is not None else None
                            if poi_data.get('Kedd zárás') is not None and str(poi_data.get('Kedd zárás')).strip() != '':
                                self.data.tu_c = str(poi_data.get('Kedd zárás')).strip() if str(poi_data.get(
                                    'Kedd zárás')).strip() is not None else None

                            if poi_data.get('Szerda nyitás') is not None and str(poi_data.get('Szerda nyitás')).strip() != '':
                                self.data.we_o = str(poi_data.get('Szerda nyitás')).strip() if str(poi_data.get(
                                    'Szerda nyitás')).strip() is not None else None
                            if poi_data.get('Szerda zárás') is not None and str(poi_data.get('Szerda zárás')).strip() != '':
                                self.data.we_c = str(poi_data.get('Szerda zárás')).strip() if str(poi_data.get(
                                    'Szerda zárás')).strip() is not None else None

                            if poi_data.get('Csütörtök nyitás') is not None and str(poi_data.get('Csütörtök nyitás')).strip() != '':
                                self.data.th_o = str(poi_data.get('Csütörtök nyitás')).strip() if str(poi_data.get(
                                    'Csütörtök nyitás')).strip() is not None else None
                            if poi_data.get('Csütörtök zárás') is not None and str(poi_data.get('Csütörtök zárás')).strip() != '':
                                self.data.th_c = str(poi_data.get('Csütörtök zárás')).strip() if str(poi_data.get(
                                    'Csütörtök zárás')).strip() is not None else None

                            if poi_data.get('Péntek nyitás') is not None and str(poi_data.get('Péntek nyitás')).strip() != '':
                                self.data.fr_o = str(poi_data.get('Péntek nyitás')).strip() if str(poi_data.get(
                                    'Péntek nyitás')).strip() is not None else None
                            if poi_data.get('Péntek zárás') is not None and str(poi_data.get('Péntek zárás')).strip() != '':
                                self.data.fr_c = str(poi_data.get('Péntek zárás')).strip() if str(poi_data.get(
                                    'Péntek zárás')).strip() is not None else None

                            if poi_data.get('Szombat nyitás') is not None and str(poi_data.get('Szombat nyitás')).strip() != '':
                                self.data.sa_o = str(poi_data.get('Szombat nyitás')).strip() if str(poi_data.get(
                                    'Szombat nyitás')).strip() is not None else None
                            if poi_data.get('Szombat zárás') is not None and str(poi_data.get('Szombat zárás')).strip() != '':
                                self.data.sa_c = str(poi_data.get('Szombat zárás')).strip() if str(poi_data.get(
                                    'Szombat zárás')).strip() is not None else None

                            if poi_data.get('Vasárnap nyitás') is not None and str(poi_data.get('Vasárnap nyitás')).strip() != '':
                                self.data.su_o = str(poi_data.get('Vasárnap nyitás')).strip() if str(poi_data.get(
                                    'Vasárnap nyitás')).strip() is not None else None
                            if poi_data.get('Vasárnap zárás') is not None and str(poi_data.get('Vasárnap zárás')).strip() != '':
                                self.data.su_c = str(poi_data.get('Vasárnap zárás')).strip() if str(poi_data.get(
                                    'Vasárnap zárás')).strip() is not None else None
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
