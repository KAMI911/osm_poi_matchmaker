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
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city, clean_javascript_variable, clean_opening_hours_2
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'https://tesco.hu/aruhazak/'


class hu_tesco():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_tesco.html'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hutescoexp', 'poi_name': 'Tesco Expressz', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'website': 'https://www.tesco.hu', 'facebook':'https://www.facebook.com/tescoaruhazak/', 'youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.tesco.hu'},
                {'poi_code': 'hutescoext', 'poi_name': 'Tesco Extra', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'website': 'https://www.tesco.hu', 'facebook':'https://www.facebook.com/tescoaruhazak/', 'youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.tesco.hu'},
                {'poi_code': 'hutescosup', 'poi_name': 'Tesco', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'website': 'https://www.tesco.hu', 'facebook':'https://www.facebook.com/tescoaruhazak/', 'youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.tesco.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            #script = soup.find('div', attrs={'data-stores':True})
            script = soup.find(attrs={'data-stores': True})
            text = json.loads(script['data-stores'])
            for poi_data in text:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['address'])
                city = clean_city(poi_data['city'])
                branch = poi_data['name']
                if 'xpres' in poi_data['name']:
                    name = 'Tesco Expressz'
                    code = 'hutescoexp'
                elif 'xtra' in poi_data['name']:
                    name = 'Tesco Extra'
                    code = 'hutescoext'
                else:
                    name = 'Tesco'
                    code = 'hutescosup'
                website = poi_data['url']
                nonstop = None
                opening = json.loads(poi_data['opening'])
                mo_o = opening['1'][0]
                th_o = opening['2'][0]
                we_o = opening['3'][0]
                tu_o = opening['4'][0]
                fr_o = opening['5'][0]
                sa_o = opening['6'][0]
                su_o = opening['0'][0]
                mo_c = opening['1'][1]
                th_c = opening['2'][1]
                we_c = opening['3'][1]
                tu_c = opening['4'][1]
                fr_c = opening['5'][1]
                sa_c = opening['6'][1]
                su_c = opening['0'][1]
                lat = poi_data['gpslat']
                lon = poi_data['gpslng']
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, None)
                original = poi_data['address']
                ref = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c, fr_c, sa_c, su_c])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
