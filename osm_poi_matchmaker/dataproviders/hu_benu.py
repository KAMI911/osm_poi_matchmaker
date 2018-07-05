# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.opening_hours import OpeningHours
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'https://benu.hu/wordpress-core/wp-admin/admin-ajax.php?action=asl_load_stores&nonce=1900018ba1&load_all=1&layout=1'


class hu_benu():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_benu.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hubenupha', 'poi_name': 'Benu gyógyszertár', 'poi_type': 'pharmacy',
                 'poi_tags': "{'amenity': 'pharmacy', 'dispensing': 'yes', 'payment:mastercard': 'yes', 'payment:visa': 'yes', 'facebook':'https://www.facebook.com/BENUgyogyszertar', 'youtube': 'https://www.youtube.com/channel/UCBLjL10QMtRHdkak0h9exqg'}",
                 'poi_url_base': 'https://www.benu.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['street'])
                if 'BENU Gyógyszertár' not in poi_data['title']:
                    name = poi_data['title'].strip()
                    branch = None
                else:
                    name = 'Benu gyógyszertár'
                    branch = poi_data['title'].strip()
                code = 'hubenupha'
                website = poi_data['description'].strip() if poi_data['description'] is not None else None
                website = website[19:]
                nonstop = None
                mo_o = None
                tu_o = None
                we_o = None
                th_o = None
                fr_o = None
                sa_o = None
                su_o = None
                mo_c = None
                tu_c = None
                we_c = None
                th_c = None
                fr_c = None
                sa_c = None
                su_c = None
                summer_mo_o = None
                summer_tu_o = None
                summer_we_o = None
                summer_th_o = None
                summer_fr_o = None
                summer_sa_o = None
                summer_su_o = None
                summer_mo_c = None
                summer_tu_c = None
                summer_we_c = None
                summer_th_c = None
                summer_fr_c = None
                summer_sa_c = None
                summer_su_c = None
                lunch_break_start = None
                lunch_break_stop = None
                t = OpeningHours(nonstop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c, fr_c, sa_c, su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o, summer_sa_o, summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c, summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunch_break_stop)
                opening_hours = t.process()
                city = clean_city(poi_data['city'])
                postcode = poi_data['postal_code'].strip()
                lat, lon = check_hu_boundary(poi_data['lat'], poi_data['lng'])
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                original = poi_data['street']
                ref = None
                if 'phone' in poi_data and poi_data['phone'] != '':
                    phone = clean_phone(poi_data['phone'])
                else:
                    phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c,
                     fr_c, sa_c, su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o, summer_sa_o, summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c,
                     summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunch_break_stop, opening_hours])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
