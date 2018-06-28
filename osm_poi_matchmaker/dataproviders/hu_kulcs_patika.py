# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'http://kulcspatika.hu/inc/getPagerContent.php?tipus=patika&kepnelkul=true&latitude=47.498&longitude=19.0399'

POST_DATA = {'kepnelkul': 'true', 'latitude': '47.498', 'longitude': '19.0399', 'tipus': 'patika'}


class hu_kulcs_patika():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_kulcs_patika.json'):
        self.session = session
        self.link = 'http://kulcspatika.hu/inc/getPagerContent.php?tipus=patika&kepnelkul=true&latitude=47.498&longitude=19.0399'
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hukulcspha', 'poi_name': 'Kulcs patika', 'poi_type': 'pharmacy',
                 'poi_tags': "{'amenity': 'pharmacy', 'dispensing': 'yes', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.kulcspatika.hu'}]
        return data

    def process(self):
        '''
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), POST_DATA)
        insert_data = []
        if soup != None:

            text = json.loads(soup.get_text())
        '''
        with open(os.path.join(self.download_cache, self.filename), 'r') as f:
            insert_data = []
            text = json.load(f)
            for poi_data in text:
                street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['cim'])
                if 'Kulcs patika' not in poi_data['nev']:
                    name = poi_data['nev'].strip()
                    branch = None
                else:
                    name = 'Kulcs patika'
                    branch = poi_data['nev'].strip()
                code = 'hukulcspha'
                website = poi_data['link'].strip() if poi_data['link'] is not None else None
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
                summer_mo_o = None
                summer_th_o = None
                summer_we_o = None
                summer_tu_o = None
                summer_fr_o = None
                summer_sa_o = None
                summer_su_o = None
                summer_mo_c = None
                summer_th_c = None
                summer_we_c = None
                summer_tu_c = None
                summer_fr_c = None
                summer_sa_c = None
                summer_su_c = None
                lunch_break_start = None
                lunck_break_stop = None
                opening_hours = None
                city = clean_city(poi_data['helyseg'])
                lat, lon = check_hu_boundary(poi_data['marker_position']['latitude'],
                                             poi_data['marker_position']['longitude'])
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon,
                                                       poi_data['irsz'].strip())
                original = poi_data['cim']
                ref = None
                phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c,
                     fr_c, sa_c, su_c, summer_mo_o, summer_th_o, summer_we_o, summer_tu_o, summer_fr_o, summer_sa_o, summer_su_o, summer_mo_c, summer_th_c, summer_we_c, summer_tu_c,
                     summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunck_break_stop, opening_hours])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
