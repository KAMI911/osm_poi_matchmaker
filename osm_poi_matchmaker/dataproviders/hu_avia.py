# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address, extract_javascript_variable, clean_phone_to_str, \
        clean_email
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_avia(DataProvider):

    def constains(self):
        self.link = 'https://www.avia.hu/kapcsolat/toltoallomasok'
        self.tags = {'brand': 'Avia', 'operator': 'AVIA Hungária Kft.', 'fuel:diesel': 'yes',
                     'fuel:octane_95': 'yes', 'contact:email': 'avia@avia.hu',
                     'contact:facebook': 'https://www.facebook.com/AVIAHungary',
                     'contact:youtube': 'https://www.youtube.com/channel/UCjvjkjf2RgmKBuTnKSXk-Rg', }
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        huaviafu = {'amenity': 'fuel'}
        huaviafu.update(self.tags)
        self.__types = [
            {'poi_code': 'huaviafu', 'poi_name': 'Avia', 'poi_type': 'fuel', 'poi_tags': huaviafu,
             'poi_url_base': 'https://www.avia.hu', 'poi_search_name': 'avia',
             'osm_search_distance_perfect': 30000,
             'osm_search_distance_safe': 800, 'osm_search_distance_unsafe': 110},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                text = json.loads(extract_javascript_variable(
                    soup, 'markers', True), strict=False)
                for poi_data in text:
                    try:
                        self.data.name = 'Avia'
                        self.data.code = 'huaviafu'
                        if self.data.city is None:
                            self.data.city = poi_data['title']
                        self.data.ref = poi_data['kutid'] if poi_data['kutid'] is not None and poi_data['kutid'] != '' \
                            else None
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data['lat'], poi_data['lng'])
                        if poi_data['cim'] is not None and poi_data['cim'] != '':
                            self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                                self.data.conscriptionnumber = extract_all_address(
                                    poi_data['cim'])
                        self.data.website = '/toltoallomas/?id={}'.format(str(poi_data['kutid'])) \
                            if poi_data['kutid'] is not None and poi_data['kutid'] != '' else None
                        self.data.original = poi_data['cim']
                        if 'tel' in poi_data and poi_data['tel'] != '':
                            self.data.phone = clean_phone_to_str(poi_data['tel'])
                        else:
                            self.data.phone = None
                        if 'email' in poi_data and poi_data['email'] != '':
                            self.data.email = clean_email(poi_data['email'])
                        else:
                            self.data.email = None
                        self.data.public_holiday_open = False
                        self.data.fuel_octane_95 = True if poi_data.get('b95') == '1' or poi_data.get('b95g') == '1' \
                            else False
                        self.data.fuel_diesel = True if poi_data.get('dies') == '1' or poi_data.get('gdies') == '1' \
                            else False
                        self.data.fuel_octane_98 = True if poi_data.get(
                            'b98') == '1' else False
                        self.data.fuel_lpg = True if poi_data.get(
                            'lpg') == '1' else False
                        self.data.fuel_e85 = True if poi_data.get(
                            'e85') == '1' else False
                        self.data.rent_lpg_bottles = True if poi_data.get(
                            'pgaz') == '1' else False
                        self.data.compressed_air = True if poi_data.get(
                            'komp') == '1' else False
                        self.data.restaurant = True if poi_data.get(
                            'etterem') == '1' else False
                        self.data.food = True if poi_data.get(
                            'bufe') == '1' else False
                        self.data.truck = True if poi_data.get(
                            'kpark') == '1' else False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
