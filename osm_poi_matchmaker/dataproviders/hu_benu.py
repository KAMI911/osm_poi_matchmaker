# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        PATTERN_FULL_URL, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_benu(DataProvider):

    def contains(self):
        self.link = 'https://benu.hu/rest/V1/enabledPharmacySearch'
        # self.link = os.path.join(config.get_directory_cache_url(), 'hu_benu.json')
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
            {'poi_code': 'hubenupha', 'poi_common_name': 'Benu gyógyszertár', 'poi_type': 'pharmacy',
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
                try:
                    text = json.loads(soup, strict=False)
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.error(f)
                for poi_data in text:
                    try:
                        if 'BENU Gyógyszertár' not in poi_data.get('name'):
                            self.data.name = poi_data.get('name').strip()
                            self.data.branch = None
                        else:
                            self.data.branch = poi_data.get('name').strip()
                        self.data.code = 'hubenupha'
                        if poi_data.get('description') is not None:
                            pu_match = PATTERN_FULL_URL.match(poi_data.get('description'))
                            self.data.website = pu_match.group(0).strip() if pu_match is not None else None
                        else:
                            self.data.website = None
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.postcode = clean_string(poi_data.get('postal_code'))
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(poi_data.get('street'))
                        self.data.original = poi_data.get('street')
                        self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
