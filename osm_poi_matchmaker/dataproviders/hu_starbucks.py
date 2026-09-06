# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import datetime
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_starbucks(DataProvider):
    """Imports Starbucks coffee shop locations in Hungary from Starbucks' own store
    locator API (www.starbucks.co.uk/api/v2/stores/), radius-searched from Hungary's
    approximate centroid (a single 250km-radius call already covers the whole
    country, confirmed against AllThePlaces' own count for this source: 40).

    Note: this API's street-address text is an English transliteration (e.g. "37 Futo
    str" for "Futó utca 37", "Kerepesi ut 61" for "Kerepesi út 61") rather than proper
    Hungarian text, and isn't reliably parseable into street/housenumber - so only
    city/postcode/coordinates are used here, not street. City names are similarly
    unaccented (e.g. "Kecskemet") and are used as-is."""

    def contains(self):
        self.link = ('https://www.starbucks.co.uk/api/v2/stores/'
                    '?filter[coordinates][latitude]=47.1625&filter[coordinates][longitude]=19.5033'
                    '&filter[radius]=250')
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'amenity': 'cafe', 'brand': 'Starbucks', 'brand:wikidata': 'Q37158',
                     'cuisine': 'coffee_shop', 'contact:website': 'https://www.starbucks.hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hustarbucks = self.tags.copy()
        self.__types = [
            {'poi_code': 'hustarbux', 'poi_common_name': 'Starbucks', 'poi_type': 'cafe',
             'poi_tags': hustarbucks, 'poi_url_base': 'https://www.starbucks.hu', 'poi_search_name': 'starbucks',
             'additional_ref_name': 'ref', 'osm_search_distance_perfect': 300,
             'osm_search_distance_safe': 100, 'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False, headers=self.headers)
            if soup is None:
                return
            for poi_data in json.loads(soup).get('data') or []:
                try:
                    attributes = poi_data.get('attributes') or {}
                    address = attributes.get('address') or {}
                    if (address.get('countryCode') or '').upper() != 'HU':
                        continue
                    self.data.code = 'hustarbux'
                    self.data.ref = clean_string(attributes.get('storeNumber'))
                    self.data.branch = clean_string(attributes.get('name'))
                    self.data.city = clean_city(address.get('city'))
                    self.data.postcode = clean_string(address.get('postalCode'))
                    coordinates = attributes.get('coordinates') or {}
                    self.data.lat, self.data.lon = check_hu_boundary(
                        coordinates.get('latitude'), coordinates.get('longitude'))
                    if attributes.get('open24x7') is True:
                        self.data.nonstop = True
                    else:
                        for rule in attributes.get('openHours') or []:
                            try:
                                weekday = datetime.date.fromisoformat(rule['date'][:10]).weekday()
                            except (KeyError, ValueError):
                                continue
                            if rule.get('open') is False:
                                continue
                            if rule.get('open24Hours') is True:
                                self.data.day_open_close(weekday, '00:00', '23:59')
                            elif rule.get('openTime') and rule.get('closeTime'):
                                self.data.day_open_close(weekday, rule['openTime'][:5], rule['closeTime'][:5])
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
