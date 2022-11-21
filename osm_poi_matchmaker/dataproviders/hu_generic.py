# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import pandas as pd
    import numpy as np
    from lxml import etree
    from osm_poi_matchmaker.dao.data_handlers import insert_city_dataframe, insert_street_type_dataframe, insert_patch_data_dataframe, insert_country_data_dataframe
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.xml import save_downloaded_xml
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

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
        xml = save_downloaded_xml('{}'.format(self.link), os.path.join(
            self.download_cache, self.filename))
        insert_data = []
        try:
            root = etree.fromstring(xml)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.error(traceback.print_exc())
            logging.error(xml)
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
        xml = save_downloaded_xml('{}'.format(self.link), os.path.join(
            self.download_cache, self.filename))
        insert_data = []
        try:
            root = etree.fromstring(xml)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.error(traceback.print_exc())
            logging.error(xml)
        for e in root.findall('streetType'):
            if e.text is not None:
                street_type = e.text.strip()
                insert_data.append(street_type)
        df = pd.DataFrame(insert_data)
        df.columns = POI_COLS_STREET_TYPE
        insert_street_type_dataframe(self.session, df)

class poi_patch_from_csv():
    """Insert CVS patch data into database poi_patch table
    """
    def __init__(self, session, filename : str='poi_patch.csv'):
        """[summary]

        Args:
            session ([type]): [description]
            filename (str, optional): Filename of CSV file to import. Defaults to 'poi_patch.csv'.
        """        
        self.session = session
        self.filename = filename

    def process(self):
        csv = save_downloaded_pd(None, os.path.join('data', self.filename), None)
        csv = csv.replace({np.nan: None})
        insert_patch_data_dataframe(self.session, csv)

class poi_country_from_csv():
    """Insert CVS country data into database country table
    """
    def __init__(self, session, filename : str='country.csv'):
        """[summary]

        Args:
            session ([type]): [description]
            filename (str, optional): Filename of CSV file to import. Defaults to 'country.csv'.
        """        
        self.session = session
        self.filename = filename

    def process(self):
        csv = save_downloaded_pd(None, os.path.join('data', self.filename), None)
        csv = csv.replace({np.nan: None})
        insert_country_data_dataframe(self.session, csv)
