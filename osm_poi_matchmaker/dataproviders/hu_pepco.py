# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
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


class hu_pepco(DataProvider):

    def contains(self):
        self.link = 'https://pepco.hu/uzleteink/uzletkereso/?type=1002&tx_pepco_mapplugin[action]=view&tx_pepco_mapplugin[controller]=Map&tx_pepco_mapplugin[loadall]=true'
        self.tags = {'shop': 'clothes', 'brand': 'Pepco', 'brand:wikidata': 'Q11815580',
                     'brand:wikipedia': 'pl:Pepco', 'contact:facebook': 'https://www.facebook.com/pepcohu/',
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
                for poi_data in text['data']:
                    try:
                        '''
                        The Pepco dataset contains all European data. Since the program cannot handle POIs outside Hungary (so far)
                        this will limit only for Hungarian POIs
                        In fact this depends on OSM extract but currently we use only Hungarian OSM extract
                        Select only Hungarian POIs
                        '''
                        if 'city' in poi_data and (poi_data['city'] == '' or
                                                query_osm_city_name(self.session, poi_data['city']) is None):
                            continue
                        elif 'city' in poi_data:
                            self.data.city = clean_city(poi_data['city'])
                        else:
                            continue
                        self.data.code = 'hupepcoclo'
                        # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                        self.data.lat, self.data.lon = \
                            check_hu_boundary(
                                poi_data['coordinates']['lat'], poi_data['coordinates']['lng'])
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(
                                poi_data.get('streetAddress'))
                        self.data.original = clean_string(poi_data.get('streetAddress'))
                        self.data.postcode = clean_string(poi_data.get('postalCode'))
                        # self.data.city = query_osm_city_name_gpd(self.session, self.data.lat, self.data.lon)
                        # Assign opening_hours
                        opening = poi_data['openingHours']
                        for i in range(0, 7):
                            if i in opening:
                                self.data.day_open(i, opening[i]['from'])
                                self.data.day_close(i, opening[i]['to'])
                        # Assign additional informations
                        self.data.phone = clean_phone_to_str(poi_data.get('phoneNumber'))
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
