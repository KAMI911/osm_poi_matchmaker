# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# point_open_<day> is the branch's own hours; the identically-shaped point_open_<day>_b
# columns are a second, separate schedule (seen non-null on some ATM-only "Mobil
# bankfiók" rows) that isn't used here since mobile-branch rows are skipped entirely.
DAY_FIELDS = [('mon', 0), ('tue', 1), ('wed', 2), ('thu', 3), ('fri', 4), ('sat', 5), ('sun', 6)]

# MKB/Takarékbank/Budapest Bank are MBH's own predecessor names (see poi_search_name below),
# not competitors, so they're deliberately left out of this list.
AVOID_NAME = '(otp|cib|k&h|kh bank|raiffeisen|unicredit|erste|gránit|sberbank|magnet|kereskedelmi és hitel|coinatm|bitcoin|dt bank)'


class hu_mbh_bank(DataProvider):
    """Imports MBH Bank branch/ATM locations in Hungary from the same JSON endpoint
    (https://www.mbhbank.hu/apps/backend/branch-and-atm) the bank's own branch/ATM
    finder widget uses."""

    def contains(self):
        self.link = 'https://www.mbhbank.hu/apps/backend/branch-and-atm'
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/120.0 Safari/537.36'}
        self.tags = {'operator': 'MBH Bank Nyrt.', 'brand': 'MBH Bank', 'brand:wikidata': 'Q124547528',
                     'contact:website': 'https://www.mbhbank.hu', 'air_conditioning': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        humbhbank = {'amenity': 'bank', 'atm': 'yes'}
        humbhbank.update(self.tags)
        humbhatm = {'amenity': 'atm'}
        humbhatm.update(self.tags)
        self.__types = [
            {'poi_code': 'humbhbank', 'poi_common_name': 'MBH Bank', 'poi_type': 'bank', 'poi_tags': humbhbank,
             'poi_url_base': 'https://www.mbhbank.hu',
             # MBH formed from a 2023 merger of MKB, Takarékbank and Budapest Bank - OSM nodes for
             # former branches often still carry one of those predecessor names, not "MBH" itself.
             'poi_search_name': '(mbh bank|mbh|mkb bank|mkb|takarékbank|takarék bank|budapest bank)',
             'poi_search_avoid_name': AVOID_NAME,
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'humbhatm', 'poi_common_name': 'MBH Bank ATM', 'poi_type': 'atm', 'poi_tags': humbhatm,
             'poi_url_base': 'https://www.mbhbank.hu',
             'poi_search_name': '(mbh bank|mbh|mkb bank|mkb|takarékbank|takarék bank|budapest bank)',
             'poi_search_avoid_name': AVOID_NAME,
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False, headers=self.headers)
            if soup is None:
                return
            text = json.loads(soup)
            for poi_data in text:
                try:
                    category = poi_data.get('point_category')
                    name = poi_data.get('point_name') or ''
                    # "Mobil bankfiók" vehicles serve dozens of towns on brief weekly stops -
                    # not fixed POIs, so skip them regardless of which category they're filed under.
                    if category == 2 or name.startswith('Mobil bankfiók'):
                        continue
                    if category == 0:
                        self.data.code = 'humbhatm'
                        self.data.public_holiday_open = True
                    elif category in (1, 3):
                        self.data.code = 'humbhbank'
                        self.data.branch = clean_string(name)
                        self.data.public_holiday_open = False
                    else:
                        logging.debug('Unexpected point_category: %s', category)
                        continue
                    self.data.ref = clean_string(poi_data.get('point_id'))
                    self.data.city = clean_city(poi_data.get('point_city'))
                    self.data.postcode = clean_string(poi_data.get('point_zip'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(poi_data.get('point_street'))
                    self.data.original = clean_string(poi_data.get('point_street'))
                    self.data.phone = clean_phone_to_str(poi_data.get('point_phone'))
                    self.data.lat, self.data.lon = check_hu_boundary(
                        poi_data.get('point_latitude'), poi_data.get('point_longitude'))
                    for day_name, day_index in DAY_FIELDS:
                        value = poi_data.get('point_open_{}'.format(day_name))
                        if not value or value == '-':
                            continue
                        try:
                            open_time, close_time = value.split('-')
                        except ValueError:
                            continue
                        self.data.day_open_close(day_index, open_time.strip(), close_time.strip())
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
