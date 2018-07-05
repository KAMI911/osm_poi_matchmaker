# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import extract_all_address
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.opening_hours import OpeningHours
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS


class hu_cib_bank():

    def __init__(self, session, link, download_cache, prefer_osm_postcode, name):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.name = name

    @staticmethod
    def types():
        data = [{'poi_code': 'hucibbank', 'poi_name': 'CIB bank', 'poi_type': 'bank',
                 'poi_tags': "{'amenity': 'bank', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.', bic': 'CIBHHUHB', 'atm': 'yes'}",
                 'poi_url_base': 'https://www.cib.hu'},
                {'poi_code': 'hucibatm', 'poi_name': 'CIB', 'poi_type': 'atm',
                 'poi_tags': "{'amenity': 'atm', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.'}",
                 'poi_url_base': 'https://www.cib.hu'}]
        return data

    def process(self):
        if self.link:
            with open(self.link, 'r') as f:
                insert_data = []
                text = json.load(f)
                for poi_data in text['results']:
                    first_element = next(iter(poi_data))
                    if self.name == 'CIB bank':
                        name = 'CIB bank'
                        code = 'hucibbank'
                    else:
                        name = 'CIB'
                        code = 'hucibatm'
                    postcode, city, street, housenumber, conscriptionnumber = extract_all_address(
                        poi_data[first_element]['address'])
                    branch = None
                    website = None
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
                    t = OpeningHours(nonstop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c, fr_c,
                                     sa_c, su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o,
                                     summer_sa_o, summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c,
                                     summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunch_break_stop)

                    opening_hours = t.process()
                    lat, lon = check_hu_boundary(poi_data[first_element]['latitude'],
                                                 poi_data[first_element]['longitude'])
                    geom = check_geom(lat, lon)
                    postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                    original = poi_data[first_element]['address']
                    ref = None
                    phone = None
                    email = None
                    insert_data.append(
                        [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                         ref, phone, email, geom, nonstop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c,
                         th_c,
                         fr_c, sa_c, su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o, summer_sa_o,
                         summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c,
                         summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunch_break_stop, opening_hours])
                if len(insert_data) < 1:
                    logging.warning('Resultset is empty. Skipping ...')
                else:
                    df = pd.DataFrame(insert_data)
                    df.columns = POI_COLS
                    insert_poi_dataframe(self.session, df)
