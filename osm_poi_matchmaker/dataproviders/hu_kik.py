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

AVOID_NAME = '(takko|pepco|sinsay|h&m|new yorker|newyorker|zara|c&a|vesper|fashion|(^| )house( |$)|(^| )gas( |$))'


class hu_kik(DataProvider):
    """Imports KiK discount clothing store locations in Hungary from KiK's own
    storefinder microservice (one call, a huge distance/limit returns every
    Hungarian store at once - no pagination needed). Previously only covered
    indirectly via hu_alltheplaces.py's generic 'kik' alltheplaces.xyz spider."""

    def contains(self):
        self.link = ('https://storefinder-microservice.kik.de/storefinder/results.json'
                    '?lat=&long=&country=hu&distance=100000&limit=100000')
        self.tags = {'shop': 'clothes', 'brand': 'KiK', 'brand:wikidata': 'Q883965',
                     'contact:website': 'https://www.kik.hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hukik = self.tags.copy()
        self.__types = [
            {'poi_code': 'hukik', 'poi_common_name': 'KiK', 'poi_type': 'clothes', 'poi_tags': hukik,
             'poi_url_base': 'https://www.kik.hu', 'poi_search_name': 'kik',
             'poi_search_avoid_name': AVOID_NAME,
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 200, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False)
            if soup is None:
                return
            text = json.loads(soup)
            stores = text.get('stores') or []
            if not stores:
                return
            results = (stores[0] or {}).get('results') or {}
            for poi_data in results.values():
                try:
                    self.data.code = 'hukik'
                    self.data.ref = clean_string(poi_data.get('filiale'))
                    self.data.lat, self.data.lon = check_hu_boundary(
                        poi_data.get('latitude'), poi_data.get('longitude'))
                    self.data.postcode = clean_string(poi_data.get('zip'))
                    self.data.city = clean_city(clean_string(poi_data.get('city')))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(poi_data.get('address'))
                    self.data.original = clean_string(poi_data.get('address'))
                    self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
