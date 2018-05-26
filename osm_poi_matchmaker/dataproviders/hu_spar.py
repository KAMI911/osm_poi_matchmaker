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
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'https://www.spar.hu/bin/aspiag/storefinder/stores?country=HU'

PATTERN_SPAR_REF = re.compile('\((.*?)\)')


class hu_spar():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_spar.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'husparexp', 'poi_name': 'Spar Expressz', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', 'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'payment':'cash', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.spar.hu'},
                {'poi_code': 'husparint', 'poi_name': 'Interspar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'payment':'cash', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.spar.hu'},
                {'poi_code': 'husparsup', 'poi_name': 'Spar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'payment':'cash', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.spar.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['address'])
                if 'xpres' in poi_data['name']:
                    name = 'Spar Expressz'
                    code = 'husparexp'
                elif 'INTER' in poi_data['name']:
                    name = 'Interspar'
                    code = 'husparint'
                elif 'market' in poi_data['name']:
                    name = 'Spar'
                    code = 'husparsup'
                else:
                    name = 'Spar'
                    code = 'husparsup'
                poi_data['name'] = poi_data['name'].replace('INTERSPAR', 'Interspar')
                poi_data['name'] = poi_data['name'].replace('SPAR', 'Spar')
                ref_match = PATTERN_SPAR_REF.search(poi_data['name'])
                ref = ref_match.group(1).strip() if ref_match is not None else None
                city = clean_city(poi_data['city'])
                postcode = poi_data['zipCode'].strip()
                branch = poi_data['name'].split('(')[0].strip()
                website = poi_data['pageUrl'].strip()
                nonstop = None
                mo_o = None
                th_o = None
                we_o = None
                tu_o = None
                fr_o = None
                sa_o = None
                su_o = None
                mo_c = None
                th_c = None
                we_c = None
                tu_c = None
                fr_c = None
                sa_c = None
                su_c = None
                lat = poi_data['latitude']
                lon = poi_data['longitude']
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                original = poi_data['address']
                phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c, fr_c, sa_c, su_c])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
