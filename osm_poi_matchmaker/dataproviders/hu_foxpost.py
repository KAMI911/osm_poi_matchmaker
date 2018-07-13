# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.enums import WeekDaysLongHUUnAccented
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'http://www.foxpost.hu/wp-content/themes/foxpost/googleapijson.php'


class hu_foxpost():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_foxpost.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [
            {'poi_code': 'hufoxpocso', 'poi_name': 'Foxpost', 'poi_type': 'vending_machine_parcel_pickup_and_mail_in',
             'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup;parcel_mail_in', 'brand': 'Foxpost', 'operator': 'FoxPost Zrt.', 'facebook': 'https://www.facebook.com/foxpostzrt', 'youtube': 'https://www.youtube.com/channel/UC3zt91sNKPimgA32Nmcu97w', 'email': 'info@foxpost.hu', 'phone': '+36 1 999 03 69', 'payment:debit_cards': 'yes', 'payment:cash': 'no'}",
             'poi_url_base': 'https://www.foxpost.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text:
                data.name = 'Foxpost'
                data.code = 'hufoxpocso'
                data.postcode = poi_data['zip'].strip()
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['street'])
                data.city = clean_city(poi_data['city'])
                data.branch = poi_data['name']
                for i in range(0, 7):
                    if poi_data['open'][WeekDaysLongHUUnAccented(i).name.lower()] is not None:
                        opening, closing = clean_opening_hours(poi_data['open'][WeekDaysLongHUUnAccented(i).name.lower()])
                        data.day_open(i, opening)
                        data.day_close(i, closing)
                    else:
                        data.day_open_close(i, None, None)
                data.original = poi_data['address']
                data.lat, data.lon = check_hu_boundary(poi_data['geolat'], poi_data['geolng'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
