# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_POSTA = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=posta'
POI_CSEKK = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
POI_CSOMAG = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postamachine',
POI_POSTA_PONT = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postapoint',


class hu_posta_json():

    def __init__(self, session, link, download_cache, filename='hu_posta.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hupostapo', 'poi_name': 'Posta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostacse', 'poi_name': 'Posta csekkautomata', 'poi_type': 'vending_machine',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'cheques', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostacso', 'poi_name': 'Posta csomagautomata', 'poi_type': 'vending_machine',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostapp', 'poi_name': 'PostaPont', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostamp', 'poi_name': 'Mobilposta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.'}",
                 'poi_url_base': 'https://www.posta.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text['items']:
                if poi_data['type'] == 'posta':
                    if 'mobilposta' in poi_data['name']:
                        name = 'Mobilposta'
                        code = 'hupostamp'
                    else:
                        name = 'Posta'
                        code = 'hupostapo'
                elif poi_data['type'] == 'csekkautomata':
                    name = 'Posta csekkautomata'
                    code = 'hupostacse'
                elif poi_data['type'] == 'postamachine':
                    name = 'Posta csomagautomata'
                    code = 'hupostacso'
                elif poi_data['type'] == 'postapoint':
                    name = 'PostaPont'
                    code = 'hupostapp'
                else:
                    logging.error('Non existing Posta type.')
                postcode = poi_data['zipCode'].strip()
                street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                city = clean_city(poi_data['city'])
                branch = poi_data['name']
                website = None
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
                geom = check_geom(poi_data['lat'], poi_data['lng'])
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
