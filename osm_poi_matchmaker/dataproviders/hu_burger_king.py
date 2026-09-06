# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_javascript_variable, extract_street_housenumber_better_2, \
        clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

DAY_WEEKDAY_HU = {'Hétfő': 0, 'Kedd': 1, 'Szerda': 2, 'Csütörtök': 3, 'Péntek': 4, 'Szombat': 5, 'Vasárnap': 6}


class hu_burger_king(DataProvider):
    """Imports Burger King restaurant locations in Hungary, scraped from the
    'bkLocations' JS object embedded on burgerking.hu/ettermeink/ - the same data the
    page's own restaurant-finder map reads.

    The combined "info" field (e.g. "Tököl, Hermina út 1-3, 2316 Magyarország") isn't
    positionally reliable - some entries have an extra "Burger King <nickname>" segment
    before the street - but the separate "address" field always matches the START of
    the correct comma-segment (just without the house number that segment may carry),
    so the street is taken from whichever info segment starts with address's text."""

    def contains(self):
        self.link = 'https://burgerking.hu/ettermeink/'
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'amenity': 'fast_food', 'brand': 'Burger King', 'brand:wikidata': 'Q177054',
                     'cuisine': 'burger', 'contact:website': 'https://burgerking.hu'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hubkfaf = self.tags.copy()
        self.__types = [
            {'poi_code': 'hubkfaf', 'poi_common_name': 'Burger King', 'poi_type': 'fastfood', 'poi_tags': hubkfaf,
             'poi_url_base': 'https://burgerking.hu', 'poi_search_name': '(burger king|bk)',
             'additional_ref_name': 'ref', 'osm_search_distance_perfect': 300,
             'osm_search_distance_safe': 100, 'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    @staticmethod
    def __resolve_street(info, address):
        segments = [p.strip() for p in (info or '').split(',')]
        if address:
            for segment in segments[1:]:
                if segment.startswith(address):
                    return segment
        return address or None

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, verify=self.verify_link, headers=self.headers)
            if soup is None:
                return
            raw = extract_javascript_variable(soup, 'bkLocations')
            if raw is None:
                logging.warning('Could not find the bkLocations JS object. Skipping ...')
                return
            text = json.loads(raw)
            for poi_data in text.get('restaurants', []):
                try:
                    info = poi_data.get('info')
                    address = clean_string(poi_data.get('address'))
                    self.data.code = 'hubkfaf'
                    self.data.ref = clean_string(poi_data.get('id'))
                    segments = [p.strip() for p in (info or '').split(',')]
                    self.data.city = clean_city(segments[0] if segments else None)
                    self.data.postcode = clean_string(poi_data.get('zipCode'))
                    street_full = self.__resolve_street(info, address)
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(street_full)
                    self.data.original = clean_string(info)
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lng'))
                    for entry in poi_data.get('openingHours') or []:
                        day_index = DAY_WEEKDAY_HU.get(entry.get('day'))
                        if day_index is not None and entry.get('opens') and entry.get('closes'):
                            self.data.day_open_close(day_index, entry.get('opens'), entry.get('closes'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
