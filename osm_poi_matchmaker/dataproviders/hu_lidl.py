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
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# Weekday reasons that reflect the store's normal recurring schedule, as opposed to a
# one-off exception (SPECIAL_DAY, HOLIDAY, TEMPORARY_CLOSING, ...) that shouldn't be
# taken as this weekday's regular hours.
REGULAR_HOUR_REASONS = ('REGULAR', 'SUNDAY_REPEAT')

AVOID_NAME = '(aldi|auchan|cba|príma|prima|coop|penny|reál|real|spar|tesco|s-market|smarket|tom market|tommarket)'


class hu_lidl(DataProvider):
    """Imports Lidl supermarket locations in Hungary from the Schwarz Group's shared
    store API (live.api.schwarz) that Lidl's own store finder uses across every
    country it operates in."""

    def contains(self):
        self.link = 'https://live.api.schwarz/odj/stores-api/v2/myapi/stores-frontend/stores' \
                   '?offset=0&country_code=HU'
        # Public, client-side API key embedded in Lidl's own store-finder web app - not a secret.
        self.headers = {'x-apikey': '16QaHsGX3Uc3JLhNlS2ZG1CmosbzVPs2'}
        self.tags = {'brand': 'Lidl', 'brand:wikidata': 'Q151954'}
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hulidlsup = {'shop': 'supermarket'}
        hulidlsup.update(self.tags)
        self.__types = [
            {'poi_code': 'hulidlsup', 'poi_common_name': 'Lidl', 'poi_type': 'shop', 'poi_tags': hulidlsup,
             'poi_url_base': 'https://www.lidl.hu', 'poi_search_name': 'lidl', 'additional_ref_name': 'ref',
             'poi_search_avoid_name': AVOID_NAME,
             'osm_search_distance_perfect': 1000, 'osm_search_distance_safe': 400},
        ]
        return self.__types

    def __parse_hours(self, opening_hours):
        for entry in (opening_hours or {}).get('items') or []:
            try:
                weekday = datetime.date.fromisoformat(entry['date'][:10]).weekday()
            except (KeyError, ValueError):
                continue
            if entry.get('reason') not in REGULAR_HOUR_REASONS:
                continue
            time_ranges = entry.get('timeRanges') or []
            if not time_ranges:
                continue
            open_time = (time_ranges[0].get('from') or '').split('T', 1)[-1][:5]
            close_time = (time_ranges[0].get('to') or '').split('T', 1)[-1][:5]
            if open_time and close_time:
                self.data.day_open_close(weekday, open_time, close_time)

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False, headers=self.headers)
            if soup is None:
                return
            text = json.loads(soup)
            for poi_data in text.get('items') or []:
                try:
                    address = poi_data.get('address') or {}
                    self.data.code = 'hulidlsup'
                    self.data.ref = clean_string(poi_data.get('objectNumber'))
                    self.data.branch = clean_string(poi_data.get('storeName'))
                    self.data.city = clean_city(address.get('city'))
                    self.data.postcode = clean_string(address.get('zip'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(address.get('streetName'))
                    self.data.original = clean_string(address.get('streetName'))
                    self.data.lat, self.data.lon = check_hu_boundary(address.get('latitude'), address.get('longitude'))
                    self.__parse_hours(poi_data.get('openingHours'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
