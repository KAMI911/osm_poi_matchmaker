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
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS
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
        data = [{'poi_code': 'hufoxpocso', 'poi_name': 'Foxpost', 'poi_type': 'vending_machine',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup;parcel_mail_in', 'brand': 'Foxpost', 'operator': 'FoxPost Zrt.', 'facebook': 'https://www.facebook.com/foxpostzrt', 'youtube': 'https://www.youtube.com/channel/UC3zt91sNKPimgA32Nmcu97w', 'email': 'info@foxpost.hu', 'phone': '+36 1 999 03 69', 'payment:debit_cards': 'yes', 'payment:cash': 'no'}",
                 'poi_url_base': 'https://www.foxpost.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                name = 'Foxpost'
                code = 'hufoxpocso'
                postcode = poi_data['zip'].strip()
                street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['street'])
                city = clean_city(poi_data['city'])
                branch = poi_data['name']
                website = None
                nonstop = None
                if poi_data['open']['hetfo'] is not None:
                    mo_o, mo_c = clean_opening_hours(poi_data['open']['hetfo'])
                else:
                    mo_o, mo_c = None, None
                if poi_data['open']['kedd'] is not None:
                    th_o, th_c = clean_opening_hours(poi_data['open']['kedd'])
                else:
                    th_o, th_c = None, None
                if poi_data['open']['szerda'] is not None:
                    we_o, we_c = clean_opening_hours(poi_data['open']['szerda'])
                else:
                    we_o, we_c = None, None
                if poi_data['open']['csutortok'] is not None:
                    tu_o, tu_c = clean_opening_hours(poi_data['open']['csutortok'])
                else:
                    tu_o, tu_c = None, None
                if poi_data['open']['pentek'] is not None:
                    fr_o, fr_c = clean_opening_hours(poi_data['open']['pentek'])
                else:
                    fr_o, fr_c = None, None
                if poi_data['open']['szombat'] is not None:
                    sa_o, sa_c = clean_opening_hours(poi_data['open']['szombat'])
                else:
                    sa_o, sa_c = None, None
                if poi_data['open']['vasarnap'] is not None:
                    su_o, su_c = clean_opening_hours(poi_data['open']['vasarnap'])
                else:
                    su_o, su_c = None, None
                original = poi_data['address']
                ref = None
                lat, lon = check_hu_boundary(poi_data['geolat'], poi_data['geolng'])
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c,
                     fr_c, sa_c, su_c])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
