# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe, search_for_postcode
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_city, clean_javascript_variable
    from osm_poi_matchmaker.libs.geo import check_geom
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


class hu_avia():

    def __init__(self, session, link, download_cache, filename='hu_avia.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def types(self):
        data = [{'poi_code': 'huaviafu', 'poi_name': 'Avia',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Avia', 'operator': 'AVIA Hungária Kft.', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'https://www.avia.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('var\s*markers\s*=\s*((.*\n)*\]\;)', re.MULTILINE)
            script = soup.find('script', text=pattern)
            m = pattern.search(script.get_text())
            data = m.group(0)
            data = data.replace("'", '"')
            data = clean_javascript_variable(data, 'markers')
            text = json.loads(data)
            for poi_data in text:
                if poi_data['cim'] is not None and poi_data['cim'] != '':
                    postcode, city, street, housenumber, conscriptionnumber = extract_all_address(poi_data['cim'])
                if city is None:
                    logging.info('There is no city name. Matching is hardly possible.')
                    if postcode is None:
                        city = clean_city(poi_data['title'])
                        postcode = search_for_postcode(self.session, city)
                name = 'Avia'
                code = 'huaviafu'
                branch = None
                if city is None:
                    city = poi_data['title']
                ref = poi_data['kutid'] if poi_data['kutid'] is not None and poi_data['kutid'] != '' else None
                geom = check_geom(poi_data['lat'], poi_data['lng'])
                website = None
                original = poi_data['cim']
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, geom])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
