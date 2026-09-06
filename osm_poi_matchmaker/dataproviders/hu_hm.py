# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_hm(DataProvider):
    """Imports H&M clothing store locations in Hungary from H&M Group's store-locator
    API (api.storelocator.hmgroup.tech), the same one hm.com's own store finder uses.

    Note: address.streetName2 (the actual street) is an English transliteration with
    the house number reordered before the street name (e.g. "1 Budai St." for "Budai
    utca 1") - not reliably parseable as Hungarian, so only city/postcode/coordinates
    are used, not street (same data-quality limit as hu_starbucks.py/hu_sinsay.py).
    address.streetName1 is always the mall/building name, used as the branch."""

    def contains(self):
        self.link = ('https://api.storelocator.hmgroup.tech/v2/brand/hm/stores/locale/en_us/country/hu'
                    '?_type=json&campaigns=true&departments=true&openinghours=true')
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'shop': 'clothes', 'brand': 'H&M', 'brand:wikidata': 'Q188326',
                     'contact:website': 'https://www.hm.com/hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huhm = self.tags.copy()
        self.__types = [
            {'poi_code': 'huhm', 'poi_common_name': 'H&M', 'poi_type': 'clothes', 'poi_tags': huhm,
             'poi_url_base': 'https://www.hm.com/hu', 'poi_search_name': 'h&m', 'additional_ref_name': 'ref',
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
            for poi_data in json.loads(soup).get('stores') or []:
                try:
                    if (poi_data.get('countryCode') or '').upper() != 'HU':
                        continue
                    address = poi_data.get('address') or {}
                    self.data.code = 'huhm'
                    self.data.ref = clean_string(poi_data.get('storeCode'))
                    name = clean_string(poi_data.get('name')) or ''
                    self.data.branch = name.removeprefix('H&M ').removeprefix('H&M Kids ') or None
                    self.data.city = clean_city(poi_data.get('city'))
                    self.data.postcode = clean_string(address.get('postCode'))
                    self.data.original = clean_string(address.get('streetName2'))
                    self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                    self.data.lat, self.data.lon = check_hu_boundary(
                        poi_data.get('latitude'), poi_data.get('longitude'))
                    for rule in poi_data.get('openingHours') or []:
                        day = rule.get('day')
                        if day and rule.get('opens') and rule.get('closes'):
                            self.data.day_open_close(day - 1, rule.get('opens'), rule.get('closes'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
