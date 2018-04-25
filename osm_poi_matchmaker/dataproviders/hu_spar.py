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
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']

PATTERN_SPAR_REF = re.compile('\((.*?)\)')


class hu_spar():

    def __init__(self, session, link, download_cache, filename='hu_spar.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'husparexp', 'poi_name': 'Spar Expressz',
                 'poi_tags': "{'shop': 'convenience', 'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'payment':'cash', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.spar.hu'},
                {'poi_code': 'husparint', 'poi_name': 'Interspar',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'payment':'cash', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.spar.hu'},
                {'poi_code': 'husparsup', 'poi_name': 'Spar',
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
                geom = check_geom(poi_data['latitude'], poi_data['longitude'])
                original = poi_data['address']
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, geom])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
