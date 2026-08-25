# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_phone_to_str, \
        clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_osm_city_name
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


WEEKDAY_INDEX = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                 'friday': 4, 'saturday': 5, 'sunday': 6}


class hu_pepco(DataProvider):
    """Imports Pepco clothing store locations in Hungary from pepco.hu's stores API."""

    def contains(self):
        self.link = 'https://pepco.hu/api/stores?market=HU'
        self.tags = {'shop': 'clothes', 'brand': 'Pepco', 'brand:wikidata': 'Q11815580',
                     'brand:wikipedia': 'pl:Pepco', 'contact:facebook': 'pepcohu',
                     'contact:website': 'https://pepco.hu/',
                     'contact:linkedin': 'https://www.linkedin.com/company/pepco-poland',
                     'contact:phone': '+36 1 701 0424', 'contact:email': 'ugyfelszolgalat@pepco.eu',
                     'operator': 'Pepkor Hungary Kft.', 'operator:addr': '1138 Budapest, Váci út 187.'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hupepcoclo = self.tags.copy()
        hupepcoclo.update(POS_HU_GEN)
        hupepcoclo.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hupepcoclo', 'poi_common_name': 'Pepco', 'poi_type': 'clothes',
             'poi_tags': hupepcoclo, 'poi_url_base': 'https://pepco.hu', 'poi_search_name': 'pepco',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 5},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                features = text.get('globalStoreDataSet', {}).get('stores', {}).get('features', [])
                for feature in features:
                    try:
                        properties = feature.get('properties', {})
                        if properties.get('is_active') is False:
                            continue
                        '''
                        The API is queried with market=HU, so the result already only contains
                        Hungarian stores. Still validate the city against the OSM extract like other
                        providers do, to filter out unmatched/invalid city names.
                        '''
                        city = properties.get('city')
                        if not city or query_osm_city_name(self.session, city) is None:
                            continue
                        self.data.city = clean_city(city)
                        self.data.code = 'hupepcoclo'
                        # Assign: code, postcode, city, name, branch, website, original, street, housenumber,
                        # conscriptionnumber, ref, geom
                        coordinates = feature.get('geometry', {}).get('coordinates') or [None, None]
                        self.data.lat, self.data.lon = check_hu_boundary(coordinates[1], coordinates[0])
                        self.data.street = clean_string(properties.get('street'))
                        self.data.housenumber = clean_string(properties.get('street_number'))
                        self.data.original = clean_string('{} {}'.format(
                            properties.get('street') or '', properties.get('street_number') or '').strip())
                        self.data.postcode = clean_string(properties.get('zip'))
                        # Assign opening_hours
                        for block in properties.get('opening_hours') or []:
                            if block.get('closed'):
                                continue
                            for day_name in block.get('days') or []:
                                day_index = WEEKDAY_INDEX.get(day_name)
                                if day_index is not None:
                                    self.data.day_open(day_index, block.get('from'))
                                    self.data.day_close(day_index, block.get('to'))
                        # Assign additional information
                        self.data.phone = clean_phone_to_str(properties.get('phone_number'))
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(feature)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
