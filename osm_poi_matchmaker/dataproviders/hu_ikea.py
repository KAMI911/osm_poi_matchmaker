# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
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

DAY_WEEKDAY = {'MON': 0, 'TUE': 1, 'WED': 2, 'THU': 3, 'FRI': 4, 'SAT': 5, 'SUN': 6}


class hu_ikea(DataProvider):
    """Imports IKEA furniture store locations in Hungary from IKEA's own per-country
    store metadata JSON (ikea.com/hu/hu/meta-data/informera/stores-detailed.json). Only
    3 stores exist in Hungary; the in-store cafe/restaurant/bistro/play-area
    sub-locations IKEA's feed also lists aren't imported separately here - there's no
    POI type for those in this project, and they're inside the same building anyway."""

    def contains(self):
        self.link = 'https://www.ikea.com/hu/hu/meta-data/informera/stores-detailed.json'
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'shop': 'furniture', 'brand': 'IKEA', 'brand:wikidata': 'Q54078',
                     'contact:website': 'https://www.ikea.com/hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huikea = self.tags.copy()
        self.__types = [
            {'poi_code': 'huikea', 'poi_common_name': 'IKEA', 'poi_type': 'furniture', 'poi_tags': huikea,
             'poi_url_base': 'https://www.ikea.com/hu', 'poi_search_name': 'ikea', 'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 500,
             'osm_search_distance_unsafe': 30},
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
                    if (poi_data.get('buClassification') or {}).get('code') == 'FFP':
                        continue
                    address = poi_data.get('address') or {}
                    self.data.code = 'huikea'
                    name = clean_string(poi_data.get('name')) or ''
                    self.data.branch = name.replace('IKEA', '').strip(' -') or None
                    self.data.city = clean_city(address.get('city'))
                    self.data.postcode = clean_string(address.get('zipCode'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(address.get('street'))
                    self.data.original = clean_string(address.get('displayAddress'))
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lng'))
                    for rule in (poi_data.get('hours') or {}).get('normal') or []:
                        day_index = DAY_WEEKDAY.get(rule.get('day'))
                        if day_index is not None and rule.get('open') and rule.get('close'):
                            self.data.day_open_close(day_index, rule.get('open'), rule.get('close'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
