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


class hu_cba():

    def __init__(self, session, link, download_cache, filename='hu_cba.html'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def types(self):
        data = [
            {'poi_code': 'hucbacon', 'poi_name': 'CBA',
             'poi_tags': "{'shop': 'convenience', 'brand': 'CBA', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.cba.hu'},
            {'poi_code': 'hucbasup', 'poi_name': 'CBA',
             'poi_tags': "{'shop': 'supermarket', 'brand': 'CBA', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.cba.hu'},
            {'poi_code': 'huprimacon', 'poi_name': 'Príma',
             'poi_tags': "{'shop': 'convenience', 'brand': 'Príma', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.prima.hu'},
            {'poi_code': 'huprimasup', 'poi_name': 'Príma',
             'poi_tags': "{'shop': 'supermarket', 'brand': 'Príma', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.prima.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('^\s*var\s*boltok_nyers.*')
            script = soup.find('script', text=pattern)
            m = pattern.match(script.get_text())
            data = m.group(0)
            data = clean_javascript_variable(data, 'boltok_nyers')
            text = json.loads(data)
            # for l in text:
            # print ('postcode: {postcode}; city: {city}; address: {address}; alt_name: {alt_name}'.format(postcode=l['A_IRSZ'], city=l['A_VAROS'], address=l['A_CIM'], alt_name=l['P_NAME']))

            for poi_data in text:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['A_CIM'])
                city = clean_city(poi_data['A_VAROS'])
                postcode = poi_data['A_IRSZ'].strip()
                branch = poi_data['P_NAME'].strip()
                name = 'Príma' if 'Príma' in branch else 'CBA'
                code = 'huprimacon' if 'Príma' in branch else 'hucbacon'
                website = None
                original = poi_data['A_CIM']
                geom = check_geom(poi_data['PS_GPS_COORDS_LAT'], poi_data['PS_GPS_COORDS_LNG'])
                ref = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, geom])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
