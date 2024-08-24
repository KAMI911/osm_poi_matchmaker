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
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_dm(DataProvider):

    def contains(self):
        self.link = 'https://services.dm.de/storedata/stores/bbox/49%2C16%2C45%2C23'
        self.tags = {'shop': 'chemist', 'operator': 'dm Kft.', 'brand': 'dm',
                     'brand:wikidata': 'Q266572', 'brand:wikipedia': 'en:Dm-drogerie markt',
                     'contact:facebook': 'https://www.facebook.com/dm.Magyarorszag',
                     'contact:youtube': 'https://www.youtube.com/user/dmMagyarorszag',
                     'contact:instagram': 'https://www.instagram.com/dm_magyarorszag',
                     'ref:vatin': 'HU11181530', 'ref:HU:vatin': '11181530-2-44', 'ref:HU:company': '13-09-078006',
                     'air_conditioning': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hudmche = self.tags.copy()
        hudmche.update(POS_HU_GEN)
        hudmche.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hudmche', 'poi_common_name': 'dm', 'poi_type': 'chemist',
             'poi_tags': hudmche, 'poi_url_base': 'https://www.dm.hu', 'poi_search_name': 'dm',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 15},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text['stores']:
                    try:
                        if poi_data.get('localeCountry').strip().upper() == 'HU':
                            self.data.code = 'hudmche'
                            self.data.postcode = clean_string(poi_data.get('address')['zip'])
                            street_tmp = poi_data.get(
                                'address')['street'].split(',')[0]
                            self.data.city = clean_city(
                                poi_data.get('address')['city'])
                            self.data.website = 'https://www.dm.hu{}'.format(
                                clean_string(poi_data.get('storeUrlPath')))
                            self.data.original = clean_string(poi_data.get('address')['street'])
                            self.data.lat, self.data.lon = \
                                check_hu_boundary(poi_data.get('location')[
                                                  'lat'], poi_data.get('location')['lon'])
                            self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                                extract_street_housenumber_better_2(
                                    street_tmp.title())
                            self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                            self.data.ref = clean_string(poi_data.get('storeNumber').strip())
                            opening = poi_data.get('openingDays')
                            try:
                                for i, d in enumerate(opening):
                                    if d.get('weekDay') is not None and 1 <= d.get('weekDay') <= 7:
                                        day = d.get('weekDay')
                                        self.data.day_open(
                                            day - 1, d.get('timeSlices')[0].get('opening'))
                                        self.data.day_close(
                                            day - 1, d.get('timeSlices')[0].get('closing'))
                            except (IndexError, KeyError):
                                logging.warning(
                                    'Exception occurred during opening hours processing')
                            self.data.public_holiday_open = False
                            self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
