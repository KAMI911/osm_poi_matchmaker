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

WEEKDAY_INDEX = {'MONDAY': 0, 'TUESDAY': 1, 'WEDNESDAY': 2, 'THURSDAY': 3, 'FRIDAY': 4, 'SATURDAY': 5, 'SUNDAY': 6}

AVOID_NAME = '(h&m|pepco|sinsay|takko|zara|c&a)'


class hu_new_yorker(DataProvider):
    """Imports New Yorker clothing store locations in Hungary from the brand's own
    global store API (api.newyorker.de/csp/stores/store/query), the same one
    newyorker.com's own store finder uses - filtered client-side to country_code=HU
    since the endpoint returns every country's stores in one response."""

    def contains(self):
        self.link = 'https://api.newyorker.de/csp/stores/store/query?limit=10000'
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'shop': 'clothes', 'brand': 'New Yorker', 'brand:wikidata': 'Q706421',
                     'contact:website': 'https://www.newyorker.de'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hunewyork = self.tags.copy()
        self.__types = [
            {'poi_code': 'hunewyork', 'poi_common_name': 'New Yorker', 'poi_type': 'clothes',
             'poi_tags': hunewyork, 'poi_url_base': 'https://www.newyorker.de',
             'poi_search_name': '(new yorker|newyorker)', 'additional_ref_name': 'ref',
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
            for poi_data in json.loads(soup).get('items') or []:
                try:
                    address = poi_data.get('address') or {}
                    if (address.get('country_code') or '').upper() != 'HU':
                        continue
                    self.data.code = 'hunewyork'
                    self.data.ref = clean_string(poi_data.get('store_id') or poi_data.get('id'))
                    headline = clean_string(address.get('headline')) or ''
                    self.data.branch = headline.removeprefix('NEW YORKER').strip(' |') or None
                    self.data.city = clean_city(address.get('city'))
                    self.data.postcode = clean_string(address.get('zip_code'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(address.get('address_line_1'))
                    self.data.original = clean_string(address.get('address_line_1'))
                    self.data.phone = clean_phone_to_str(address.get('phone'))
                    location = poi_data.get('location') or {}
                    self.data.lat, self.data.lon = check_hu_boundary(
                        location.get('latitude'), location.get('longitude'))
                    for rule in poi_data.get('opening_times') or []:
                        day_index = WEEKDAY_INDEX.get((rule.get('week_day') or '').upper())
                        if day_index is not None and rule.get('opening') and rule.get('closing'):
                            self.data.day_open_close(day_index, rule.get('opening'), rule.get('closing'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
