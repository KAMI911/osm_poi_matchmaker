# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import datetime
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

AVOID_NAME = '(otp|cib|k&h|kh bank|raiffeisen|unicredit|mbh|mkb|takarékbank|budapest bank|gránit|sberbank|magnet|cetelem)'


class hu_erste_bank(DataProvider):
    """Imports Erste Bank branch/ATM locations in Hungary from the same JSON locator
    API (www.erstebank.hu/bin/erstegroup/gemesgapi/locations/...) Erste's own
    branch/ATM finder page uses, one instance per location type (BRANCH/ATM)."""

    def __init__(self, session, download_cache, location_type):
        # Set before calling the base __init__, which calls contains() (needs
        # self.location_type already set) and initialises self.data.
        self.location_type = location_type
        super().__init__(session, download_cache)

    def contains(self):
        self.link = ('https://www.erstebank.hu/bin/erstegroup/gemesgapi/locations/'
                    'gem_site_location_locations-hu-ebh?types={}&items=1000&page=0&language=hu'
                    '&longitude=19.040235&latitude=47.497912').format(self.location_type)
        self.tags = {'brand': 'Erste Bank', 'brand:wikidata': 'Q696867',
                     'contact:website': 'https://www.erstebank.hu'}
        self.filetype = FileType.json
        self.filename = 'hu_erste_bank.json' if self.location_type == 'BRANCH' else 'hu_erste_atm.json'

    def types(self):
        huerstebk = {'amenity': 'bank', 'atm': 'yes'}
        huerstebk.update(self.tags)
        huerstatm = {'amenity': 'atm'}
        huerstatm.update(self.tags)
        self.__types = [
            {'poi_code': 'huerstebk', 'poi_common_name': 'Erste Bank', 'poi_type': 'bank', 'poi_tags': huerstebk,
             'poi_url_base': 'https://www.erstebank.hu', 'poi_search_name': '(erste bank|erste)',
             'poi_search_avoid_name': AVOID_NAME, 'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'huerstatm', 'poi_common_name': 'Erste Bank ATM', 'poi_type': 'atm', 'poi_tags': huerstatm,
             'poi_url_base': 'https://www.erstebank.hu', 'poi_search_name': '(erste bank|erste)',
             'poi_search_avoid_name': AVOID_NAME, 'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def __parse_hours(self, opening_hours_with_dates):
        seen_days = set()
        for entry in opening_hours_with_dates or []:
            try:
                weekday = datetime.datetime.strptime(entry['date'], '%d.%m.%Y').weekday()
            except (KeyError, ValueError):
                continue
            if weekday in seen_days:
                continue
            periods = entry.get('periods') or []
            if not periods:
                continue
            seen_days.add(weekday)
            self.data.day_open_close(weekday, periods[0].get('open'), periods[0].get('close'))

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False)
            if soup is None:
                return
            text = json.loads(soup)
            is_atm = self.location_type == 'ATM'
            for poi_data in text:
                try:
                    address = poi_data.get('address') or {}
                    self.data.code = 'huerstatm' if is_atm else 'huerstebk'
                    self.data.ref = clean_string(poi_data.get('ouId'))
                    description = clean_string(poi_data.get('description'))
                    if is_atm and description is not None and description.endswith(' ATM'):
                        description = description[:-len(' ATM')].strip() or None
                    self.data.branch = description
                    self.data.city = clean_city(address.get('city'))
                    self.data.postcode = clean_string(address.get('zipCode'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(address.get('street'))
                    self.data.original = clean_string(address.get('street'))
                    location = poi_data.get('location') or {}
                    self.data.lat, self.data.lon = check_hu_boundary(
                        location.get('latitude'), location.get('longitude'))
                    self.__parse_hours(poi_data.get('openingHoursWithDates'))
                    self.data.public_holiday_open = is_atm
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
