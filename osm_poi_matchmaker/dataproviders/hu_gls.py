# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    import re
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours, \
        clean_string, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.enums import WeekDaysLongHUUnAccented
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_gls(DataProvider):

    def contains(self):
        self.link = 'https://csomag.hu/api/parcel-shops'
        self.tags = {'brand': 'GLS', 'operator': 'GLS General Logistics Systems Hungary Kft.',
                     'operator:addr': '2351 Alsónémedi, Európa utca 2.', 'ref:vatin': 'HU12369410',
                     'ref:HU:vatin': '12369410-2-44', 'ref:HU:company': '13-09-111755',
                     'contact:facebook': 'https://www.facebook.com/GLSHungaryKft/',
                     'contact:youtube': 'https://www.youtube.com/channel/UC-Lv4AkW50HM80ZZQEq8Sqw/',
                     'contact:email': 'info@gls-hungary.com', 'contact:phone': '+36 29 886 700',
                     'contact:mobile': '+36 20 890 0660',
                     'payment:contactless': 'yes', 'payment:mastercard': 'yes', 'payment:visa': 'yes',
                     'payment:cash': 'no', }
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        huglscso = {
            'amenity': 'parcel_locker',
            'brand:wikidata': 'Q366182',
            'parcel_mail_in': 'yes',
            'parcel_pickup': 'yes',
            'colour': 'blue;grey',
            'material': 'metal',
            'refrigerated': 'no',
        }
        huglspp = {'post_office': 'post_partner', 'post_office:brand': 'GLS CsomagPont',
                    'post_office:brand:wikidata': 'Q366182',
                    'post_office:parcel_pickup': 'yes', 'refrigerated': 'no'}
        huglscso.update(POS_HU_GEN)
        huglscso.update(self.tags)
        self.__types = [
            {'poi_code': 'huglscso', 'poi_common_name': 'GLS', 'poi_type': 'vending_machine_parcel_locker_and_mail_in',
             'poi_tags': huglscso, 'poi_url_base': 'https://gls-group.com', 'poi_search_name': 'gls',
             'poi_search_avoid_name': '(alzabox|alza|dpd|pick pack|postapont|easybox|sameday|foxpost|mpl|express one|z-box)', 'export_poi_name': False,
             'osm_search_distance_perfect': 600, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 2},
            {'poi_code': 'huglspp', 'poi_common_name': 'GLS', 'poi_type': 'post_partner',
             'poi_tags': huglspp, 'poi_url_base': 'https://gls-group.com', 'poi_search_name': 'gls',
             'poi_search_avoid_name': '(alzabox|alza|dpd|pick pack|postapont|easybox|sameday|foxpost|mpl|express one|z-box)',
             'export_poi_name': False,
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
                        if poi_data.get('features').get('isParcelLocker') == True:
                            self.data.code = 'huglscso'
                            self.data.public_holiday_open = True
                        elif poi_data.get('features').get('isParcelLocker') == False:
                            self.data.code = 'huglspp'
                            self.data.public_holiday_open = False
                        else:
                            logging.critical('Non matching poi code. Invalid state.')
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('location')[0], poi_data.get('location')[1])
                        self.data.postcode = clean_string(poi_data.get('contact').get('zipCode'))
                        self.data.city = clean_city(poi_data.get('contact').get('city'))
                        self.data.branch = clean_string(poi_data.get('name').split('|')[0])
                        self.data.branch = re.sub('^GLS automata', '', self.data.branch, flags=re.IGNORECASE)
                        self.data.branch = re.sub('\\(.*\\)', '', self.data.branch)
                        self.data.branch = self.data.branch.replace('Csak bankkártyás fizetés', '')
                        self.data.branch = clean_string(self.data.branch)
                        self.data.ref = poi_data.get('id')
                        self.data.original = poi_data.get('contact').get('address')
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            poi_data.get('contact').get('address'))
                        self.data.phone = clean_phone_to_str(poi_data.get('contact').get('phone'))
                        self.data.email = clean_phone_to_str(poi_data.get('contact').get('email'))
                        self.data.description = clean_string(poi_data.get('description')) if len(('name').split('|')) <= 1 else \
                            clean_string(';'.join(poi_data.get('name').split('|')[1:]))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
