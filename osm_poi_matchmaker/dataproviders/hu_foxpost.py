# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city, clean_javascript_variable
    from osm_poi_matchmaker.libs.geo import check_geom
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


class hu_foxpost():

    def __init__(self, session, link, download_cache, filename='hu_foxpost.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def types(self):
        data = [{'poi_code': 'hufoxpocso', 'poi_name': 'Foxpost',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup;parcel_mail_in', 'brand': 'Foxpost', operator: 'FoxPost Zrt.', 'contact:facebook': 'https://www.facebook.com/foxpostzrt', 'contact:youtube': 'https://www.youtube.com/channel/UC3zt91sNKPimgA32Nmcu97w', 'contact:email': 'info@foxpost.hu', 'contact:phone': '+36 1 999 03 69', 'payment:debit_cards': 'yes', 'payment:cash': 'no'}",
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
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['street'])
                city = clean_city(poi_data['city'])
                branch = poi_data['name']
                website = None
                mo = poi_data['open']['hetfo'].strip() if poi_data['open']['hetfo'] is not None else None
                th = poi_data['open']['kedd'].strip() if poi_data['open']['kedd'] is not None else None
                we = poi_data['open']['szerda'].strip() if poi_data['open']['szerda'] is not None else None
                tu = poi_data['open']['csutortok'].strip() if poi_data['open']['csutortok'] is not None else None
                fr = poi_data['open']['pentek'].strip() if poi_data['open']['pentek'] is not None else None
                sa = poi_data['open']['szombat'].strip() if poi_data['open']['szombat'] is not None else None
                su = poi_data['open']['vasarnap'].strip() if poi_data['open']['vasarnap'] is not None else None
                original = poi_data['address']
                ref = None
                geom = check_geom(poi_data['geolat'], poi_data['geolng'])
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, geom])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
