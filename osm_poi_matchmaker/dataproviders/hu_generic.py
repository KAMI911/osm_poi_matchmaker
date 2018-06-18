# -*- coding: utf-8 -*-

try:
    import traceback
    import os
    import pandas as pd
    from lxml import etree
    from osm_poi_matchmaker.dao.data_handlers import insert_city_dataframe, insert_street_type_dataframe
    from osm_poi_matchmaker.libs.xml import save_downloaded_xml
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS_CITY = ['city_post_code', 'city_name']
POI_COLS_STREET_TYPE = ['street_type']


class hu_city_postcode():

    def __init__(self, session, link):
        self.session = session
        self.link = link

    def process(self):
        xl = pd.ExcelFile(self.link)
        df = xl.parse("Települések")
        del df['Településrész']
        insert_city_dataframe(self.session, df)
        big_cities = [['Budapest', 'Bp.u.'],
                      ['Miskolc', 'Miskolc u.'],
                      ['Debrecen', 'Debrecen u.'],
                      ['Szeged', 'Szeged u.'],
                      ['Pécs', 'Pécs u.'],
                      ['Győr', 'Győr u.']
                      ]
        for city, sheet in big_cities:
            df = xl.parse(sheet)
            df.columns.values[0] = 'city_post_code'
            df['city_name'] = city
            df = df[['city_post_code', 'city_name']]
            df.drop_duplicates('city_post_code', keep='first', inplace=True)
            insert_city_dataframe(self.session, df)


class hu_city_postcode_from_xml():

    def __init__(self, session, link, download_cache, filename='hu_city_postcode.xml'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def process(self):
        xml = save_downloaded_xml('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        root = etree.fromstring(xml)
        for e in root.findall('zipCode'):
            cities = e[1].text
            postcode = e[0].text.strip()
            for i in cities.split('|'):
                insert_data.append([postcode, i.strip()])
        df = pd.DataFrame(insert_data)
        df.columns = POI_COLS_CITY
        insert_city_dataframe(self.session, df)


class hu_street_types_from_xml():

    def __init__(self, session, link, download_cache, filename='hu_street_types.xml'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def process(self):
        xml = save_downloaded_xml('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        root = etree.fromstring(xml)
        for e in root.findall('streetType'):
            if e.text is not None:
                street_type = e.text.strip()
                insert_data.append(street_type)
        df = pd.DataFrame(insert_data)
        df.columns = POI_COLS_STREET_TYPE
        insert_street_type_dataframe(self.session, df)
