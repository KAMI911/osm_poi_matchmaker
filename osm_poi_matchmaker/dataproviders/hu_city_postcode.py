# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_city_dataframe
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


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
