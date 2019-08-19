# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    from dao.data_handlers import insert_poi_dataframe
    from libs.soup import save_downloaded_soup
    from libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours
    from libs.geo import check_hu_boundary
    from libs.osm import query_postcode_osm_external
    from libs.poi_dataset import POIDataset
    from utils.enums import WeekDaysLongHUUnAccented
    from utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_foxpost(DataProvider):


    def constains(self):
        self.link = 'http://www.foxpost.hu/wp-content/themes/foxpost/googleapijson.php'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [
            {'poi_code': 'hufoxpocso', 'poi_name': 'Foxpost', 'poi_type': 'vending_machine_parcel_pickup_and_mail_in',
             'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup;parcel_mail_in', 'brand': 'Foxpost', 'operator': 'FoxPost Zrt.', 'addr:country': 'HU', 'facebook': 'https://www.facebook.com/foxpostzrt', 'youtube': 'https://www.youtube.com/channel/UC3zt91sNKPimgA32Nmcu97w', 'email': 'info@foxpost.hu', 'phone': '+36 1 999 03 69', 'payment:contactless': 'yes', 'payment:mastercard': 'yes', 'payment:visa': 'yes', 'payment:cash': 'no'}",
             'poi_url_base': 'https://www.foxpost.hu', 'poi_search_name': 'foxpost'}]
        return self.__types


    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup != None:
                text = json.loads(soup.get_text())
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
                    self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                                self.data.postcode)
                    self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            traceback.print_exc()
            logging.error(e)
