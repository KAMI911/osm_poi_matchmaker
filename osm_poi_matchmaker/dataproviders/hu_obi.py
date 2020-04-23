# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, clean_email
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_obi(DataProvider):


    def constains(self):
        self.link = 'https://www.obi.hu/storeLocatorRest/v1/stores/getAllByCountry/hu/hu?fields=name,address,phone,services,hours,storeNumber,path,email'
        self.POI_COMMON_TAGS = "'shop': 'doityourself', 'brand': 'OBI', 'brand:wikidata': 'Q300518', " \
                               "'brand:wikipedia': 'en:Obi (store)', 'operator': 'OBI Hungary Retail Kft.', " \
                               "'operator:addr': '1097 Budapest, Könyves Kálmán körút 12-14', " \
                               "'ref:vatin:hu': '13136062-2-44', 'ref:vatin': 'HU13136062', " \
                               " 'wheelchair': 'yes', 'air_conditioning': 'yes', "
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [{'poi_code': 'huobidiy', 'poi_name': 'OBI', 'poi_type': 'doityourself',
                 'poi_tags': "{" + self.POI_COMMON_TAGS + POS_HU_GEN + PAY_CASH + "}",
                 'poi_url_base': 'https://www.obi.hu', 'poi_search_name': 'obi', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 15}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text.get('stores'):
                    self.data.name = 'obi'
                    self.data.code = 'huobidiy'
                    self.data.postcode = poi_data['address']['zip'].strip()
                    self.data.city = clean_city(poi_data['address']['city'])
                    self.data.original = poi_data['address']['street']
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['address']['lat'], poi_data['address']['lon'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(poi_data['address']['street'])
                    if 'phone' in poi_data and poi_data.get('phone') != '':
                        self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                    if 'storeNumber' in poi_data and poi_data.get('storeNumber') != '':
                        self.data.ref = poi_data.get('storeNumber').strip()
                    if 'email' in poi_data and poi_data.get('email') != '':
                        self.data.email = clean_email(poi_data.get('email'))
                    if 'path' in poi_data and poi_data.get('path') != '':
                        self.data.website = poi_data.get('path')
                    # TODO: opening hour parser for poi_data.get('hours'), format is like:
                    #  Hétfő - Szombat: 8:00 - 20:00\nVasárnap: 08:00 - 18:00
                    # self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
