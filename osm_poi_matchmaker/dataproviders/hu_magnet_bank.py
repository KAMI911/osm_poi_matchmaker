# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    import os
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_string, clean_email, \
        clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# The old fiokkereso page URL only ever loaded the Vue app shell; the branch list itself comes
# from this REST resource (found in the site's portal.js bundle). The response schema doesn't
# carry the old ATM/branch "type" split any more (all 29 entries are branches; none currently
# have the keszpenzBefizetoAtm - "cash deposit ATM" - flag set), so this only imports branches now.
API_URL = 'https://www.magnetbank.hu/MagNetWeb/resources/bankfiok?osz=true'
# Opening-hours day codes (Hétfő..Péntek) mapped to our Monday-first day index; the bank has no
# weekend hours to map.
DAY_CODE_TO_INDEX = {'H': 0, 'K': 1, 'SZ': 2, 'CS': 3, 'P': 4}


class hu_magnet_bank(DataProvider):

    def contains(self):
        self.link = API_URL
        self.tags = {'brand': 'MagNet Bank', 'brand:wikidata': 'Q17379757', 'bic': 'HBWEHUHB',
                     'brand:wikipedia': 'hu:MagNet Bank', 'operator': 'MagNet Magyar Közösségi Bank Zrt.',
                     'operator:addr': '1062 Budapest, Andrássy út 98.', 'contact:fax': '+36 1 428 8889',
                     'ref:HU:company': '01-10-046111', 'ref:vatin': 'HU14413591',
                     'ref:HU:vatin': '14413591-4-44', }
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        humagnbank = {'amenity': 'bank',
                      'atm': 'yes', 'air_conditioning': 'yes', }
        humagnbank.update(self.tags)
        humagnatm = {'amenity': 'atm'}
        humagnatm.update(self.tags)
        self.__types = [
            {'poi_code': 'humagnbank', 'poi_common_name': 'MagNet Bank', 'poi_type': 'bank',
             'poi_tags': humagnbank, 'poi_url_base': 'https://www.magnetbank.hu',
             'poi_search_name': '(magnet bank|magnetbank)', 'osm_search_distance_perfect': 2000,
             'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 10},
            {'poi_code': 'humagnatm', 'poi_common_name': 'MagNet Bank ATM', 'poi_type': 'atm',
             'poi_tags': humagnatm, 'poi_url_base': 'https://www.magnetbank.hu',
             'poi_search_name': '(magnet bank|magnetbank|magnet bank atm|magnet atm)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 80, 'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text.get('bankfiokRestList') or []:
                    try:
                        if not poi_data.get('mukodo'):
                            continue
                        self.data.code = 'humagnbank'
                        self.data.public_holiday_open = False
                        self.data.branch = clean_string(poi_data.get('honlaponNev'))
                        self.data.email = clean_email(poi_data.get('email'))
                        self.data.phone = clean_phone_to_str(poi_data.get('telefonszam'))
                        self.data.ref = clean_string(poi_data.get('kod'))
                        self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                            self.data.conscriptionnumber = extract_all_address_waxeye(
                                poi_data.get('cim'))
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('latitude'), poi_data.get('longitude'))
                        self.data.original = clean_string(poi_data.get('cim'))
                        for opening_day in poi_data.get('nyitvatartasList') or []:
                            day_index = DAY_CODE_TO_INDEX.get(opening_day.get('nap'))
                            if day_index is not None:
                                self.data.day_open(day_index, opening_day.get('nyitas'))
                                self.data.day_close(day_index, opening_day.get('zaras'))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
