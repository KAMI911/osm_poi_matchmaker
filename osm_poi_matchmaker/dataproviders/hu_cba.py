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
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city, clean_javascript_variable, clean_opening_hours_2
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_COLS = poi_array_structure.POI_COLS
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
                nonstop = None
                mo_o = clean_opening_hours_2(poi_data['PS_OPEN_FROM_1']) if poi_data['PS_OPEN_FROM_1'] is not None else None
                th_o = clean_opening_hours_2(poi_data['PS_OPEN_FROM_2']) if poi_data['PS_OPEN_FROM_2'] is not None else None
                we_o = clean_opening_hours_2(poi_data['PS_OPEN_FROM_3']) if poi_data['PS_OPEN_FROM_3'] is not None else None
                tu_o = clean_opening_hours_2(poi_data['PS_OPEN_FROM_4']) if poi_data['PS_OPEN_FROM_4'] is not None else None
                fr_o = clean_opening_hours_2(poi_data['PS_OPEN_FROM_5']) if poi_data['PS_OPEN_FROM_5'] is not None else None
                sa_o = clean_opening_hours_2(poi_data['PS_OPEN_FROM_6']) if poi_data['PS_OPEN_FROM_6'] is not None else None
                su_o = clean_opening_hours_2(poi_data['PS_OPEN_FROM_7']) if poi_data['PS_OPEN_FROM_7'] is not None else None
                mo_c = clean_opening_hours_2(poi_data['PS_OPEN_TO_1']) if poi_data['PS_OPEN_TO_1'] is not None else None
                th_c = clean_opening_hours_2(poi_data['PS_OPEN_TO_2']) if poi_data['PS_OPEN_TO_2'] is not None else None
                we_c = clean_opening_hours_2(poi_data['PS_OPEN_TO_3']) if poi_data['PS_OPEN_TO_3'] is not None else None
                tu_c = clean_opening_hours_2(poi_data['PS_OPEN_TO_4']) if poi_data['PS_OPEN_TO_4'] is not None else None
                fr_c = clean_opening_hours_2(poi_data['PS_OPEN_TO_5']) if poi_data['PS_OPEN_TO_5'] is not None else None
                sa_c = clean_opening_hours_2(poi_data['PS_OPEN_TO_6']) if poi_data['PS_OPEN_TO_6'] is not None else None
                su_c = clean_opening_hours_2(poi_data['PS_OPEN_TO_7']) if poi_data['PS_OPEN_TO_7'] is not None else None
                original = poi_data['A_CIM']
                lat = poi_data['PS_GPS_COORDS_LAT']
                lon = poi_data['PS_GPS_COORDS_LNG']
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                ref = None
                phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c, fr_c, sa_c, su_c])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
