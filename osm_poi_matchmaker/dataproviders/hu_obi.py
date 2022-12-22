# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_email, clean_string, clean_url
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_obi(DataProvider):

    def contains(self):
        self.link = 'https://www.obi.hu/storeLocatorRest/v1/stores/getAllByCountry/hu/hu?fields=name,address,phone,services,hours,storeNumber,path,email'
        self.tags = {'shop': 'doityourself', 'brand': 'OBI', 'brand:wikidata': 'Q300518',
                     'brand:wikipedia': 'en:Obi (store)', 'operator': 'OBI Hungary Retail Kft.',
                     'operator:addr': '1097 Budapest, Könyves Kálmán körút 12-14',
                     'ref:vatin:hu': '13136062-2-44', 'ref:vatin': 'HU13136062',
                     'wheelchair': 'yes', 'air_conditioning': 'yes', }
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        huobidiy = self.tags.copy()
        huobidiy.update(POS_HU_GEN)
        huobidiy.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'huobidiy', 'poi_common_name': 'OBI', 'poi_type': 'doityourself',
             'poi_tags': huobidiy, 'poi_url_base': 'https://www.obi.hu', 'poi_search_name': 'obi',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 15},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text.get('stores'):
                    try:
                        self.data.name = 'OBI'
                        self.data.code = 'huobidiy'
                        self.data.postcode = clean_string(poi_data['address']['zip'].strip())
                        self.data.city = clean_city(poi_data['address']['city'])
                        self.data.original = clean_string(poi_data['address']['street'])
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data['address']['lat'],
                                                                        poi_data['address']['lon'])
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            poi_data['address']['street'])
                        if 'phone' in poi_data and poi_data.get('phone') != '':
                            self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                        self.data.ref = clean_string(poi_data.get('storeNumber'))
                        self.data.email = clean_email(poi_data.get('email'))
                        if 'path' in poi_data and poi_data.get('path') != '':
                            self.data.website = clean_url(poi_data.get('path'))
                        # TODO: opening hour parser for poi_data.get('hours'), format is like:
                        #  Hétfő - Szombat: 8:00 - 20:00\nVasárnap: 08:00 - 18:00
                        # self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
