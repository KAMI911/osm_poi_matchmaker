# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS


class hu_benu():

    def __init__(self, session, link, download_cache, filename='hu_benu.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hubenupha', 'poi_name': 'Benu gyógyszertár', 'poi_type': 'pharmacy',
                 'poi_tags': "{'amenity': 'pharmacy', 'dispensing': 'yes', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.benu.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['street'])
                if 'BENU Gyógyszertár' not in poi_data['title']:
                    name = poi_data['title'].strip()
                    branch = None
                else:
                    name = 'Benu gyógyszertár'
                    branch = poi_data['title'].strip()
                code = 'hubenupha'
                website = poi_data['description'].strip() if poi_data['description'] is not None else None
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
                city = clean_city(poi_data['city'])
                postcode = poi_data['postal_code'].strip()
                geom = check_geom(poi_data['lat'], poi_data['lng'])
                original = poi_data['street']
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
