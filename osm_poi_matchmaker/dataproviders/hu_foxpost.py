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
        clean_string, extract_all_address_waxeye
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.enums import WeekDaysLongHUUnAccented
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

PATTERN_REF = re.compile('([A-Z]{2}\d{2,4})')

class hu_foxpost(DataProvider):

    def contains(self):
        self.link = 'https://cdn.foxpost.hu/foxpost_terminals_extended_v3.json'
        self.tags = {'operator:addr': '1097 Budapest, Könyves Kálmán krt. 12-14. kék lépcsőház 2. emelet',}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hufoxpocso = {
            'brand': 'Foxpost A-BOX',
            'brand:wikidata': 'Q126538316',
            'operator': 'FoxPost Kft.',
            'amenity': 'parcel_locker',
            'parcel_mail_in': 'yes',
            'parcel_pickup': 'yes',
            'colour': 'red',
            'material': 'metal',
            'refrigerated': 'no',
            'ref:vatin': 'HU25034644',
            'ref:HU:vatin': '25034644-2-10',
            'ref:HU:company': '10-10-020309',
            'brand:wikidata': 'Q126538316',
            'contact:facebook': 'foxpostzrt',
            'contact:youtube': 'https://www.youtube.com/channel/UC3zt91sNKPimgA32Nmcu97w',
            'contact:email': 'info@foxpost.hu',
            'contact:phone': '+36 1 999 0369',}
        hufoxpocso.update(POS_HU_GEN)
        hufoxpocso.update(self.tags)
        hufoxpzcso = {
            'brand': 'Foxpost Z-BOX',
            'brand:wikidata': 'Q126538316',
            'operator': 'FoxPost Kft.',
            'amenity': 'parcel_locker',
            'parcel_mail_in': 'yes',
            'parcel_pickup': 'yes',
            'colour': 'red',
            'material': 'metal',
            'refrigerated': 'no',
            'ref:vatin': 'HU25034644',
            'ref:HU:vatin': '25034644-2-10',
            'ref:HU:company': '10-10-020309',
            'brand:wikidata': 'Q126538316',
            'contact:facebook': 'foxpostzrt',
            'contact:youtube': 'https://www.youtube.com/channel/UC3zt91sNKPimgA32Nmcu97w',
            'contact:email': 'info@foxpost.hu',
            'contact:phone': '+36 1 999 0369',}
        hufoxpzcso.update(POS_HU_GEN)
        hufoxpzcso.update(self.tags)

        hupackecso = {
            'brand': 'Packeta Z-BOX',
            'brand:wikidata': 'Q121537464',
            'operator': 'Packeta Hungary Kft.',
            'operator:wikidata': 'Q67809905',
            'amenity': 'parcel_locker',
            'parcel_mail_in': 'yes',
            'parcel_pickup': 'yes',
            'colour': 'red;white',
            'material': 'metal',
            'refrigerated': 'no',
            'ref:vatin': 'HU25140550',
            'ref:HU:vatin': '25140550-2-43',
            'ref:HU:company': '01–09–202186',
            'contact:facebook': 'packetaHU',
            'contact:instagram': 'packeta.hu',
            'contact:youtube': 'https://www.youtube.com/channel/UCEkepxS8AaIRqfFICQyGlvg',
            'contact:linkedin': 'https://www.linkedin.com/company/packeta-hu-kft/',
            'contact:phone': '+36 1 400 8806',}
        hufoxpzcso.update(POS_HU_GEN)
        hufoxpzcso.update(self.tags)

        hupacketpp = {
            'post_office': 'post_partner',
            'post_office:brand': 'Packeta Z-Pont',
            'post_office:brand:wikidata': 'Q121537464',
            'post_office:parcel_pickup': 'yes',
            'refrigerated': 'no',}

        self.__types = [
            {'poi_code': 'hufoxpocso', 'poi_common_name': 'Foxpost A-BOX', 'poi_type': 'vending_machine_parcel_locker_and_mail_in',
             'poi_tags': hufoxpocso, 'poi_url_base': 'https://www.foxpost.hu', 'poi_search_name': 'foxpost',
             'poi_search_avoid_name': '(alzabox|alza|dpd|gls|pick pack|postapont|easybox|sameday|mpl|express one|z-box|z-pont)', 'export_poi_name': False,
             'osm_search_distance_perfect': 600, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 2},
            {'poi_code': 'hufoxpzcso', 'poi_common_name': 'Foxpost Z-BOX', 'poi_type': 'vending_machine_parcel_locker_and_mail_in',
             'poi_tags': hufoxpzcso, 'poi_url_base': 'https://www.foxpost.hu', 'poi_search_name': 'foxpost',
             'poi_search_avoid_name': '(alzabox|alza|dpd|gls|pick pack|postapont|easybox|sameday|mpl|express one|a-box|z-pont)', 'export_poi_name': False,
             'osm_search_distance_perfect': 600, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 2},
            {'poi_code': 'hupackecso', 'poi_common_name': 'Z-Box',
             'poi_type': 'vending_machine_parcel_locker_and_mail_in',
             'poi_tags': hupackecso, 'poi_url_base': 'https://www.packeta.hu/zbox', 'poi_search_name': 'Z-Box',
             'poi_search_avoid_name': '(alzabox|alza|foxpost|dpd|gls|pick pack|postapont|easybox|sameday|mpl|express one|a-box|z-pont)',
             'export_poi_name': False,
             'osm_search_distance_perfect': 600, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 2},
            {'poi_code': 'hupacketpp', 'poi_common_name': 'Z-Box',
             'poi_type': 'post_partner',
             'poi_tags': hupacketpp, 'poi_url_base': 'https://www.packeta.hu/', 'poi_search_name': 'Z-Pont',
             'poi_search_avoid_name': '(alzabox|alza|foxpost|dpd|gls|pick pack|postapont|easybox|sameday|mpl|express one|a-box|z-box)',
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
                for poi_data in text:
                    try:
                        variant = poi_data.get('variant')
                        if not variant:
                            continue
                        variant_upper = variant.upper()
                        variant_map = {
                            'FOXPOST A-BOX': 'hufoxpocso',
                            'FOXPOST Z-BOX': 'hufoxpzcso',
                            'PACKETA Z-PONT': 'hupacketpp',
                            'PACKETA Z-BOX': 'hupackecso'
                        }
                        if variant_upper in variant_map:
                            self.data.code = variant_map[variant_upper]
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('geolat'), poi_data.get('geolng'))
                        self.data.postcode = clean_string(poi_data.get('zip'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.branch = clean_string(poi_data.get('name'))
                        self.data.description = clean_string(poi_data.get('findme'))
                        if len(poi_data.get('paymentOptions')) <= 1:
                            payment_method = 'only'
                        else:
                            payment_method = 'yes'
                        if poi_data.get('paymentOptions') and 'card' in poi_data.get('paymentOptions'):
                            self.tags.update(POS_HU_GEN)
                        if poi_data.get('paymentOptions') and 'link' in poi_data.get('paymentOptions'):
                            self.tags.update({'payment:link': payment_method})
                        if poi_data.get('paymentOptions') and 'cash' in poi_data.get('paymentOptions'):
                            self.tags.update({'payment:cash': payment_method})
                        if self.data.description:
                            if 'kültéri' in self.data.description:
                                self.data.nonstop = True
                            else:
                                for i in range(0, 7):
                                    if poi_data['open'][WeekDaysLongHUUnAccented(i).name.lower()] is not None:
                                        opening, closing = clean_opening_hours(
                                            poi_data['open'][WeekDaysLongHUUnAccented(i).name.lower()])
                                        self.data.day_open(i, opening)
                                        self.data.day_close(i, closing)
                                    else:
                                        self.data.day_open_close(i, None, None)
                        self.data.original = poi_data.get('address')
                        self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                            self.data.conscriptionnumber = extract_all_address_waxeye(poi_data.get('address'))
                        self.data.public_holiday_open = False
                        self.data.ref = clean_string(poi_data.get('operator_id'))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
