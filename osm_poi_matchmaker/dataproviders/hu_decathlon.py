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

DAY_FIELDS = [('1', 0), ('2', 1), ('3', 2), ('4', 3), ('5', 4), ('6', 5), ('7', 6)]


class hu_decathlon(DataProvider):
    """Imports Decathlon sporting-goods store locations in Hungary from Woosmap's
    public store-search API (the same api.woosmap.com/stores endpoint, HU-specific
    API key, that decathlon.hu's own store finder uses)."""

    def contains(self):
        self.link = 'https://api.woosmap.com/stores?key=woos-f4e1c407-8623-32f6-aaf9-c586e8ef2337' \
                   '&stores_by_page=300&page=1'
        self.headers = {'Origin': 'https://www.decathlon.hu'}
        self.tags = {'shop': 'sports', 'brand': 'Decathlon', 'brand:wikidata': 'Q509349',
                     'contact:website': 'https://www.decathlon.hu'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hudecathl = self.tags.copy()
        self.__types = [
            {'poi_code': 'hudecathl', 'poi_common_name': 'Decathlon', 'poi_type': 'sports',
             'poi_tags': hudecathl, 'poi_url_base': 'https://www.decathlon.hu', 'poi_search_name': 'decathlon',
             'additional_ref_name': 'ref', 'osm_search_distance_perfect': 1000,
             'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False, headers=self.headers)
            if soup is None:
                return
            for feature in json.loads(soup).get('features') or []:
                try:
                    props = feature.get('properties') or {}
                    address = props.get('address') or {}
                    self.data.code = 'hudecathl'
                    self.data.ref = clean_string(props.get('store_id'))
                    self.data.branch = clean_string(props.get('name'))
                    self.data.city = clean_city(address.get('city'))
                    self.data.postcode = clean_string(address.get('zipcode'))
                    lines = address.get('lines') or []
                    street_full = lines[0] if lines else None
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(street_full)
                    self.data.original = clean_string(street_full)
                    contact = props.get('contact') or {}
                    self.data.phone = clean_phone_to_str(contact.get('phone'))
                    coords = (feature.get('geometry') or {}).get('coordinates') or []
                    if len(coords) >= 2:
                        self.data.lat, self.data.lon = check_hu_boundary(coords[1], coords[0])
                    usual_hours = ((props.get('opening_hours') or {}).get('usual')) or {}
                    for day_key, day_index in DAY_FIELDS:
                        for rule in usual_hours.get(day_key) or []:
                            if rule.get('start') and rule.get('end'):
                                self.data.day_open_close(day_index, rule.get('start'), rule.get('end'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(feature)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
