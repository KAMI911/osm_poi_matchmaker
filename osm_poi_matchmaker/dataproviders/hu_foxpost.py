# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.enums import WeekDaysLongHUUnAccented
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_foxpost(DataProvider):

    def constains(self):
        self.link = 'https://cdn.foxpost.hu/foxpost_terminals_extended_v3.json'
        self.POI_COMMON_TAGS = "'brand': 'Foxpost', 'operator': 'FoxPost Zrt.', " \
                               "'operator:addr': '3200 Gyöngyös, Batsányi János utca 9.', 'ref:vatin': 'HU25034644', " \
                               "'ref:vatin:hu': '25034644-2-10', 'ref:HU:company': '10 10 020309', " \
                               "'contact:facebook': 'https://www.facebook.com/foxpostzrt', " \
                               "'contact:youtube': 'https://www.youtube.com/channel/UC3zt91sNKPimgA32Nmcu97w', " \
                               "'contact:email': 'info@foxpost.hu', 'phone': '+36 1 999 03 69', " \
                               "'payment:contactless': 'yes', 'payment:mastercard': 'yes', 'payment:visa': 'yes', " \
                               "'payment:cash': 'no' ,"
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [
            {'poi_code': 'hufoxpocso', 'poi_name': 'Foxpost', 'poi_type': 'vending_machine_parcel_pickup_and_mail_in',
             'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup;parcel_mail_in', " +
                         self.POI_COMMON_TAGS + "}",
             'poi_url_base': 'https://www.foxpost.hu', 'poi_search_name': 'foxpost',
             'osm_search_distance_perfect': 1000, 'osm_search_distance_safe': 400, 'osm_search_distance_unsafe': 20}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    self.data.name = 'Foxpost'
                    self.data.code = 'hufoxpocso'
                    self.data.postcode = poi_data['zip'].strip()
                    self.data.city = clean_city(poi_data['city'])
                    self.data.branch = poi_data['name']
                    for i in range(0, 7):
                        if poi_data['open'][WeekDaysLongHUUnAccented(i).name.lower()] is not None:
                            opening, closing = clean_opening_hours(
                                poi_data['open'][WeekDaysLongHUUnAccented(i).name.lower()])
                            self.data.day_open(i, opening)
                            self.data.day_close(i, closing)
                        else:
                            self.data.day_open_close(i, None, None)
                    self.data.original = poi_data['address']
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['geolat'], poi_data['geolng'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data['street'])
                    self.data.public_holiday_open = False
                    self.data.description = poi_data.get('findme')
                    self.data.add()
        except Exception as e:
            logging.exception('Exception occurred')

            logging.error(e)
