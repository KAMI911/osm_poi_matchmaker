# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# bershka/pull&bear/stradivarius/massimo dutti/oysho are Zara's own sibling brands
# under the same Inditex group, distinct standalone stores in Hungary too.
AVOID_NAME = ('(h&m|new yorker|newyorker|pepco|sinsay|takko|c&a|bershka|pull&bear|pull and bear|'
             'stradivarius|massimo dutti|oysho)')


class hu_zara(DataProvider):
    """Imports Zara clothing store locations in Hungary from Zara's own store-locator
    API (zara.com/hu/en/stores-locator/extended/search), the same one the site's store
    finder uses. A single 1000km-radius search from Hungary's centroid already covers
    the whole country (8 stores, matching AllThePlaces' own count for this source)."""

    def contains(self):
        self.link = ('https://www.zara.com/hu/en/stores-locator/extended/search'
                    '?lat=47.1817585&lng=19.5060937&isDonationOnly=false&showOnlyPickup=false'
                    '&showStoresCapacity=false&radius=1000&ajax=true')
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/120.0 Safari/537.36',
                        'Referer': 'https://www.zara.com/hu/en/z-stores-st1404.html'}
        self.tags = {'shop': 'clothes', 'brand': 'Zara', 'brand:wikidata': 'Q147662',
                     'contact:website': 'https://www.zara.com/hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huzara = self.tags.copy()
        self.__types = [
            {'poi_code': 'huzara', 'poi_common_name': 'Zara', 'poi_type': 'clothes', 'poi_tags': huzara,
             'poi_url_base': 'https://www.zara.com/hu', 'poi_search_name': 'zara', 'additional_ref_name': 'ref',
             'poi_search_avoid_name': AVOID_NAME,
             'osm_search_distance_perfect': 500, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False, headers=self.headers)
            if soup is None:
                return
            for poi_data in json.loads(soup):
                try:
                    if (poi_data.get('countryCode') or '').upper() != 'HU':
                        continue
                    self.data.code = 'huzara'
                    self.data.ref = clean_string(poi_data.get('id'))
                    name = clean_string(poi_data.get('commercialName') or poi_data.get('name')) or ''
                    self.data.branch = name.removeprefix('ZARA ') or None
                    self.data.city = clean_city(poi_data.get('city'))
                    self.data.postcode = clean_string(poi_data.get('zipCode'))
                    address_lines = poi_data.get('addressLines') or []
                    street_full = ' '.join(address_lines).replace(',', '')
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(street_full)
                    self.data.original = clean_string(', '.join(address_lines))
                    phones = poi_data.get('phones') or []
                    self.data.phone = clean_phone_to_str(phones[0]) if phones else None
                    self.data.lat, self.data.lon = check_hu_boundary(
                        poi_data.get('latitude'), poi_data.get('longitude'))
                    for entry in poi_data.get('openingHours') or []:
                        week_day = entry.get('weekDay')
                        if not week_day:
                            continue
                        for interval in entry.get('openingHoursInterval') or []:
                            if interval.get('openTime') and interval.get('closeTime'):
                                self.data.day_open_close(week_day - 1, interval['openTime'], interval['closeTime'])
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
