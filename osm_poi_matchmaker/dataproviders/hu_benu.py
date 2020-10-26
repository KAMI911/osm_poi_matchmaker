# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        PATTERN_FULL_URL
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_benu(DataProvider):

    def constains(self):
        self.link = 'https://benu.hu/wordpress-core/wp-admin/admin-ajax.php?action=asl_load_stores&nonce=1900018ba1&load_all=1&layout=1'
        self.tags = {'brand': 'Benu gyógyszertár', 'dispensing': 'yes',
                     'contact:facebook': 'https://www.facebook.com/BENUgyogyszertar',
                     'contact:youtube': 'https://www.youtube.com/channel/UCBLjL10QMtRHdkak0h9exqg',
                     'air_conditioning': 'yes', }
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hubenupha = {'amenity': 'pharmacy'}
        hubenupha.update(self.tags)
        self.__types = [
            {'poi_code': 'hubenupha', 'poi_name': 'Benu gyógyszertár', 'poi_type': 'pharmacy',
             'poi_tags': hubenupha, 'poi_url_base': 'https://benu.hu',
             'poi_search_name': '(benu gyogyszertár|benu)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 20, 'preserve_original_name': True},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(str(soup))
                for poi_data in text:
                    try:
                        if 'BENU Gyógyszertár' not in poi_data.get('title'):
                            self.data.name = poi_data.get('title').strip()
                            self.data.branch = None
                        else:
                            self.data.name = 'Benu gyógyszertár'
                            self.data.branch = poi_data.get('title').strip()
                        self.data.code = 'hubenupha'
                        if poi_data.get('description') is not None:
                            pu_match = PATTERN_FULL_URL.match(poi_data.get('description'))
                            self.data.website = pu_match.group(0).strip() if pu_match is not None else None
                        else:
                            self.data.website = None
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.postcode = poi_data.get('postal_code').strip()
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            poi_data.get(('street')))
                        self.data.original = poi_data.get('street')
                        if 'phone' in poi_data and poi_data.get('phone') != '':
                            self.data.phone = clean_phone_to_str(
                                poi_data.get('phone'))
                        else:
                            self.data.phone = None
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.error(e)
                        logging.error(poi_data)
                        logging.exception('Exception occurred')

        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')
