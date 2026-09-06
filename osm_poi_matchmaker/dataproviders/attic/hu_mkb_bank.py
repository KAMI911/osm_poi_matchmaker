# -*- coding: utf-8 -*-
import math

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
        extract_phone_number, clean_url, clean_string, clean_city
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_mkb_bank(DataProvider):
    """Imports MKB Bank branch locations in Hungary from a locally cached CSV file (hu_mkb_bank.csv)."""

    def contains(self):
        self.link = os.path.join(
            config.get_directory_cache_url(), 'hu_mkb_bank.csv')
        self.tags = {'brand': 'MKB Bank', 'brand:wikidata': 'Q916185', 'bic': 'MKKBHUHB',
                     'brand:wikipedia': 'hu:MKB Bank', 'operator': 'MKB Bank Nyrt.',
                     'operator:addr': '1056 Budapest, Váci u. 38.', 'ref:vatin': 'HU10011922',
                     'ref:HU:vatin': '10011922-4-44', 'ref:HU:company': '01-10-040952', 'air_conditioning': 'yes'}
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
            {'poi_code': 'humkbbank', 'poi_common_name': 'MKB Bank', 'poi_type': 'bank',
             'poi_tags': humkbbank, 'poi_url_base': 'https://www.mkb.hu',
             'poi_search_name': '(mkb|mkb bank)',
             'poi_search_avoid_name': '(otpbank|otp|otp bank|raiffeisenbank|raiffeisen bank|kh bank|k&h|raiffeisen|budapest bank|takarék bank|takarék)',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 300, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'humkbatm', 'poi_common_name': 'MKB Bank ATM', 'poi_type': 'atm',
             'poi_tags': humkbatm,
             'poi_url_base': 'https://www.mkb.hu',
             'poi_search_name': '(mkb|mkb bank|mkb bank atm|mkb atm)',
             'poi_search_avoid_name': '(otp atm|otp|raiffeisen|raiffeisen atm|kh bank|k&h|budapest bank|takarék bank|takarék)',
             'additional_ref_name': 'ref',
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
                            self.data.code = 'humkbbank'
                            self.data.public_holiday_open = False
                        else:
                            self.data.code = 'humkbatm'
                            self.data.public_holiday_open = True
                        self.data.postcode = clean_string(poi_data.get('Körzetszám'))
                        self.data.city = clean_city(poi_data.get('Város'))
                        # pd.read_csv().to_dict('records') turns an empty cell into a float
                        # NaN, not None/'' - clean_string() catches that (and non-str types in
                        # general) before .replace() gets a chance to crash on a non-string.
                        raw_lat = clean_string(poi_data.get('Földrajzi szélesség'))
                        raw_lon = clean_string(poi_data.get('Földrajzi hosszúság'))
                        self.data.lat, self.data.lon = check_hu_boundary(
                            raw_lat.replace(',', '.') if raw_lat is not None else None,
                            raw_lon.replace(',', '.') if raw_lon is not None else None)
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(poi_data['Cím'])
                        self.data.original = clean_string(poi_data.get('Cím'))
                        self.data.email = clean_email(poi_data.get('E-mail cím'))
                        self.data.website = clean_url(poi_data.get('Időpontfoglalás URL'))
                        self.data.ref = clean_string(poi_data.get('ATM / Fiók azonosítója'))
                        self.data.description = clean_string(poi_data.get('Megjegyzés'))
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
                            # clean_string() converts pd.read_csv()'s empty-cell NaN (a float,
                            # not None/'') to None as well as trimming/blanking real values -
                            # the previous 'is not None and str(...).strip() != ""' checks let
                            # NaN straight through, str()-ing it into the literal text 'nan'.
                            self.data.mo_o = clean_string(poi_data.get('Hétfő nyitás'))
                            self.data.mo_c = clean_string(poi_data.get('Hétfő zárás'))
                            self.data.tu_o = clean_string(poi_data.get('Kedd nyitás'))
                            self.data.tu_c = clean_string(poi_data.get('Kedd zárás'))
                            self.data.we_o = clean_string(poi_data.get('Szerda nyitás'))
                            self.data.we_c = clean_string(poi_data.get('Szerda zárás'))
                            self.data.th_o = clean_string(poi_data.get('Csütörtök nyitás'))
                            self.data.th_c = clean_string(poi_data.get('Csütörtök zárás'))
                            self.data.fr_o = clean_string(poi_data.get('Péntek nyitás'))
                            self.data.fr_c = clean_string(poi_data.get('Péntek zárás'))
                            self.data.sa_o = clean_string(poi_data.get('Szombat nyitás'))
                            self.data.sa_c = clean_string(poi_data.get('Szombat zárás'))
                            self.data.su_o = clean_string(poi_data.get('Vasárnap nyitás'))
                            self.data.su_c = clean_string(poi_data.get('Vasárnap zárás'))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
