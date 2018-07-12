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
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_javascript_variable, clean_opening_hours_2, clean_phone
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'http://www.cba.hu/uzletlista/'


class hu_cba():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_cba.html'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [
            {'poi_code': 'hucbacon', 'poi_name': 'CBA', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'convenience', 'brand': 'CBA', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.cba.hu'},
            {'poi_code': 'hucbasup', 'poi_name': 'CBA', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'supermarket', 'brand': 'CBA', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.cba.hu'},
            {'poi_code': 'huprimacon', 'poi_name': 'Príma', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'convenience', 'brand': 'Príma', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.prima.hu'},
            {'poi_code': 'huprimasup', 'poi_name': 'Príma', 'poi_type': 'shop',
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
            data = POIDataset()
            for poi_data in text:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['A_CIM'])
                data.city = clean_city(poi_data['A_VAROS'])
                data.postcode = poi_data['A_IRSZ'].strip()
                data.branch = poi_data['P_NAME'].strip()
                data.name = 'Príma' if 'Príma' in data.branch else 'CBA'
                data.code = 'huprimacon' if 'Príma' in data.branch else 'hucbacon'
                for i in range(0, 7):
                    data.day_open(i, clean_opening_hours_2(poi_data['PS_OPEN_FROM_{}'.format(i + 1)]) if poi_data[
                                                                                                             'PS_OPEN_FROM_{}'.format(
                                                                                                                 i + 1)] is not None else None)
                    data.day_close(i, clean_opening_hours_2(poi_data['PS_OPEN_TO_{}'.format(i + 1)]) if poi_data[
                                                                                                            'PS_OPEN_TO_{}'.format(
                                                                                                                i + 1)] is not None else None)
                data.original = poi_data['A_CIM']
                data.lat, data.lon = check_hu_boundary(poi_data['PS_GPS_COORDS_LAT'], poi_data['PS_GPS_COORDS_LNG'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                if 'PS_PUBLIC_TEL' in poi_data and poi_data['PS_PUBLIC_TEL'] != '':
                    data.phone = clean_phone(poi_data['PS_PUBLIC_TEL'])
                else:
                    data.phone = None
                if 'PS_PUBLIC_EMAIL' in poi_data and poi_data['PS_PUBLIC_EMAIL'] != '':
                    data.email = poi_data['PS_PUBLIC_EMAIL']
                else:
                    data.email = None
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
