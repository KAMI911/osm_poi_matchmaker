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
        clean_javascript_variable, clean_opening_hours
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'https://www.rossmann.hu/uzletkereso'


class hu_rossmann():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_rossmann.html', **kwargs):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename
        if 'verify_link' in kwargs:
            self.verify_link = kwargs['verify_link']
        else:
            self.verify_link = None

    @staticmethod
    def types():
        data = [{'poi_code': 'hurossmche', 'poi_name': 'Rossmann', 'poi_type': 'chemist',
                 'poi_tags': "{'shop': 'chemist', 'operator': 'Rossmann Magyarország Kft.', 'brand':'Rossmann', 'facebook':'https://www.facebook.com/Rossmann.hu', 'youtube': 'https://www.youtube.com/channel/UCmUCPmvMLL3IaXRBtx7-J7Q', 'instagram':'https://www.instagram.com/rossmann_hu/', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.rossmann.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), None,
                                    self.verify_link)
        insert_data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('^\s*var\s*places.*')
            script = soup.find('script', text=pattern)
            m = pattern.match(script.get_text())
            data = m.group(0)
            data = clean_javascript_variable(data, 'places')
            text = json.loads(data)
            for poi_data in text:
                poi_data = poi_data['addresses'][0]
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                name = 'Rossmann'
                code = 'hurossmche'
                city = clean_city(poi_data['city'])
                postcode = poi_data['zip'].strip()
                branch = None
                website = None
                nonstop = False
                if poi_data['business_hours']['monday'] is not None:
                    mo_o, mo_c = clean_opening_hours(poi_data['business_hours']['monday'])
                else:
                    mo_o, mo_c = None, None
                if poi_data['business_hours']['tuesday'] is not None:
                    th_o, th_c = clean_opening_hours(poi_data['business_hours']['tuesday'])
                else:
                    th_o, th_c = None, None
                if poi_data['business_hours']['wednesday'] is not None:
                    we_o, we_c = clean_opening_hours(poi_data['business_hours']['wednesday'])
                else:
                    we_o, we_c = None, None
                if poi_data['business_hours']['thursday'] is not None:
                    tu_o, tu_c = clean_opening_hours(poi_data['business_hours']['thursday'])
                else:
                    tu_o, tu_c = None, None
                if poi_data['business_hours']['friday'] is not None:
                    fr_o, fr_c = clean_opening_hours(poi_data['business_hours']['friday'])
                else:
                    fr_o, fr_c = None, None
                if poi_data['business_hours']['saturday'] is not None:
                    sa_o, sa_c = clean_opening_hours(poi_data['business_hours']['saturday'])
                else:
                    sa_o, sa_c = None, None
                if poi_data['business_hours']['sunday'] is not None:
                    su_o, su_c = clean_opening_hours(poi_data['business_hours']['sunday'])
                else:
                    su_o, su_c = None, None
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
                lat, lon = check_hu_boundary(poi_data['position'][0], poi_data['position'][1])
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                original = poi_data['address']
                ref = None
                phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c,
                     fr_c, sa_c, su_c, summer_mo_o, summer_th_o, summer_we_o, summer_tu_o, summer_fr_o, summer_sa_o, summer_su_o, summer_mo_c, summer_th_c, summer_we_c, summer_tu_c,
                     fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunck_break_stop, opening_hours])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
