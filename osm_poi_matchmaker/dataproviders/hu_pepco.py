# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    from dao.data_handlers import insert_poi_dataframe
    from libs.soup import save_downloaded_soup
    from libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, clean_string
    from libs.geo import check_hu_boundary
    from libs.osm import query_postcode_osm_external, query_osm_city_name_gpd, query_osm_city_name
    from libs.poi_dataset import POIDataset
    from utils.data_provider import DataProvider
    from libs.osm_tag_sets import POS_OTP
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


class hu_pepco(DataProvider):


    def constains(self):
        self.link = 'https://pepco.hu/uzleteink/uzletkereso/?type=1002&tx_pepco_mapplugin[action]=view&tx_pepco_mapplugin[controller]=Map&tx_pepco_mapplugin[loadall]=true'
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hupepcoclo', 'poi_name': 'Pepco', 'poi_type': 'clothes',
                 'poi_tags': "{'shop': 'clothes', 'brand': 'Pepco', 'brand:wikidata': 'Q11815580', 'brand:wikipedia': 'pl:Pepco', 'contact:facebook': 'https://www.facebook.com/pepcohu/', 'contact:website':'https://pepco.hu/', 'contact:linkedin': 'https://www.linkedin.com/company/pepco-poland', 'contact:phone': '+36 1 701 0424', 'contact:email': 'ugyfelszolgalat@pepco.eu', 'operator': 'Pepkor Hungary Kft.', 'operator:addr': '1138 Budapest, Váci út 187.', 'payment:cash': 'yes', 'payment:contactless': 'yes', 'payment:mastercard': 'yes', 'payment:visa': 'yes' }",
                 'poi_url_base': 'https://pepco.hu', 'poi_search_name': 'tesco', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup != None:
                text = json.loads(soup.get_text())
                for poi_data in text['data']:
                    '''
                    The Pepco dataset contains all European data. Since the program cannot handle POIs outside Hungary (so far)
                    this will limit only for Hungarian POIs
                    In fact this depends on OSM extract but currently we use only Hungarian OSM extract
                    Select only Hungarian POIs
                    '''
                    if 'city' in poi_data and (poi_data['city'] == '' or query_osm_city_name(self.session, poi_data['city']) is None):
                        continue
                    elif 'city' in poi_data:
                        self.data.city = poi_data['city']
                    else:
                        continue
                    self.data.name = 'Pepco'
                    self.data.code = 'hupepcoclo'
                    # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['coordinates']['lat'], poi_data['coordinates']['lng'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(poi_data.get('streetAddress'))
                    self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                                 poi_data.get('postalCode'))
                    # self.data.city = query_osm_city_name_gpd(self.session, self.data.lat, self.data.lon)
                    # Assign opening_hours
                    opening = poi_data['openingHours']
                    for i in range(0, 7):
                        if i in opening:
                            self.data.day_open(i, opening[i]['from'])
                            self.data.day_close(i, opening[i]['to'])
                    # Assign additional informations
                    if 'phone' in poi_data and poi_data['phone'] != '':
                        self.data.phone = clean_phone_to_str(poi_data.get('phoneNumber'))
                    self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(logging.error(e))
