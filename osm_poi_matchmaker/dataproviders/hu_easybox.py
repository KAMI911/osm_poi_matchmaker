# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours, \
        clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_easybox(DataProvider):

    def contains(self):
        self.link = 'https://sameday.hu/wp/wp-admin/admin-ajax.php?action=get_lockers_request&search='
        self.tags = {'brand': 'easybox', 'operator': 'Delivery Solutions Zrt.',
                     'operator:addr': '1097 Budapest, Könyves Kálmán körút 34.', 'ref:vatin': 'HU32041848',
                     'ref:HU:vatin': '32041848-2-43', 'ref:HU:company': '01-10-141982',
                     'brand:wikidata': 'Q114496224', 'operator:wikidata': 'Q131107197',
                     'contact:email': 'info@sameday.hu', 'contact:phone': '+36 1 374 3890',
                     'payment:contactless': 'yes', 'payment:maestro': 'yes',
                     'payment:mastercard': 'yes', 'payment:mastercard_contactless': 'yes', 'payment:mastercard_electronic': 'yes',
                     'payment:visa': 'yes', 'payment:visa_electron': 'yes',
                     'payment:cash': 'no',}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hueasybcso = {'amenity': 'parcel_locker', 'parcel_mail_in': 'yes', 'parcel_pickup': 'yes',
                      'colour': 'red;white', 'material': 'metal', 'refrigerated': 'no'}
        hueasybcso.update(POS_HU_GEN)
        hueasybcso.update(self.tags)
        self.__types = [
            {'poi_code': 'hueasybcso', 'poi_common_name': 'Easybox', 'poi_type': 'vending_machine_parcel_locker_and_mail_in',
             'poi_tags': hueasybcso, 'poi_url_base': 'https://sameday.hu', 'poi_search_name': 'easybox',
             'poi_search_avoid_name': '(alzabox|alza|dpd|gls|pick pack|postapont|foxpost|mpl|express one|z-box)', 'export_poi_name': False,
             'osm_search_distance_perfect': 600, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 2},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text.get('data'):
                    try:
                        if poi_data.get('countryId') == 237:
                            self.data.code = 'hueasybcso'
                            self.data.lat, self.data.lon = check_hu_boundary(
                                poi_data.get('lat'), poi_data.get('lng'))
                            self.data.city = clean_city(poi_data.get('city'))
                            self.data.branch = clean_string(poi_data.get('name'))
                            self.data.original = poi_data.get('address')
                            self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                                poi_data.get('address'))
                            self.data.public_holiday_open = False
                            self.data.ref = clean_string(poi_data.get('lockerId'))
                            self.data.add()
                        else:
                            continue
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
