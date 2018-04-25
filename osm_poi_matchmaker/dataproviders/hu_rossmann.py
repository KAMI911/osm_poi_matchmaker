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


class hu_rossmann():

    def __init__(self, session, link, download_cache, filename='hu_rossmann.html', **kwargs):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename
        if 'verify_link' in kwargs:
            self.verify_link = kwargs['verify_link']

    @staticmethod
    def types():
        data = [{'poi_code': 'hurossmche', 'poi_name': 'Rossmann',
                 'poi_tags': "{'shop': 'chemist', 'operator': 'Rossmann Magyarország Kft.', 'brand':'Rossmann', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.rossmann.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), None, self.verify_link)
        insert_data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('^\s*var\s*places.*')
            script = soup.find('script', text=pattern)
            m = pattern.match(script.get_text())
            data = m.group(0)
            data = clean_javascript_variable(data, 'places')
            text = json.loads(data)
            for poi_data in text:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['addresses'][0]['address'])
                name = 'Rossmann'
                code = 'hurossmche'
                city = clean_city(poi_data['city'])
                postcode = poi_data['addresses'][0]['zip'].strip()
                branch = None
                website = None,
                geom = check_geom(poi_data['addresses'][0]['position'][0], poi_data['addresses'][0]['position'][1])
                original = poi_data['addresses'][0]['address']
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
