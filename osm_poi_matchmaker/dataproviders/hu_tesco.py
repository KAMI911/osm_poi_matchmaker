# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch', 'poi_website', 'original',
            'poi_addr_street',
            'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_ref', 'poi_geom']


class hu_tesco():

    def __init__(self, session, link, download_cache, filename='hu_tesco.html'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def types(self):
        data = [{'poi_code': 'hutescoexp', 'poi_name': 'Tesco Expressz',
                 'poi_tags': "{'shop': 'convenience', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'contact:website': 'https://www.tesco.hu', 'contact:facebook':'https://www.facebook.com/tescoaruhazak/', 'contact:youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.tesco.hu'},
                {'poi_code': 'hutescoext', 'poi_name': 'Tesco Extra',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'contact:website': 'https://www.tesco.hu', 'contact:facebook':'https://www.facebook.com/tescoaruhazak/', 'contact:youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.tesco.hu'},
                {'poi_code': 'hutescosup', 'poi_name': 'Tesco',
                 'poi_tags': "{'shop': 'supermarket', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'contact:website': 'https://www.tesco.hu', 'contact:facebook':'https://www.facebook.com/tescoaruhazak/', 'contact:youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.tesco.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        data = []
        insert_data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            table = soup.find('table', attrs={'class': 'tescoce-table'})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                link = cols[0].find('a').get('href') if cols[0].find('a') != None else []
                cols = [element.text.strip() for element in cols]
                cols[0] = cols[0].split('\n')[0]
                del cols[-1]
                del cols[-1]
                cols.append(link)
                data.append(cols)
            for poi_data in data:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(poi_data[3])
                tesco_replace = re.compile('(expressz{0,1})', re.IGNORECASE)
                poi_data[0] = tesco_replace.sub('Expressz', poi_data[0])
                if 'xpres' in poi_data[0]:
                    name = 'Tesco Expressz'
                    code = 'hutescoexp'
                elif 'xtra' in poi_data[0]:
                    name = 'Tesco Extra'
                    code = 'hutescoext'
                else:
                    name = 'Tesco'
                    code = 'hutescosup'
                poi_data[0] = poi_data[0].replace('TESCO', 'Tesco')
                poi_data[0] = poi_data[0].replace('Bp.', 'Budapest')
                postcode = poi_data[1].strip()
                city = clean_city(poi_data[2].split(',')[0])
                branch = poi_data[0]
                website = poi_data[4]
                original = poi_data[3]
                geom = None
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
