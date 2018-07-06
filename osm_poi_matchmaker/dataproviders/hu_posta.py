# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from lxml import etree
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.xml import save_downloaded_xml
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
POI_DATA = 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/PostInfo.xml'
DAYS = {0: 'Hétfő', 1: 'Kedd', 2: 'Szerda', 3: 'Csütörtök', 4: 'Péntek', 5: 'Szombat', 6: 'Vasárnap'}


def dict_search(my_dict, lookup):
    for key, value in my_dict.items():
        if lookup in value:
            return key


class hu_posta():
    # Processing http://httpmegosztas.posta.hu/PartnerExtra/OUT/PostInfo.xml file
    def __init__(self, session, download_cache, prefer_osm_postcode, filename='PostInfo.xml'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hupostapo', 'poi_name': 'Posta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'facebook': 'https://www.facebook.com/MagyarPosta/', 'youtube': 'https://www.youtube.com/user/magyarpostaofficial'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostacse', 'poi_name': 'Posta csekkautomata', 'poi_type': 'vending_machine_cheques',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'cheques', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'facebook': 'https://www.facebook.com/MagyarPosta/', 'youtube': 'https://www.youtube.com/user/magyarpostaofficial'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostacso', 'poi_name': 'Posta csomagautomata',
                 'poi_type': 'vending_machine_parcel_pickup',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'facebook': 'https://www.facebook.com/MagyarPosta/', 'youtube': 'https://www.youtube.com/user/magyarpostaofficial'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostapp', 'poi_name': 'PostaPont', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'facebook': 'https://www.facebook.com/MagyarPosta/', 'youtube': 'https://www.youtube.com/user/magyarpostaofficial'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostamp', 'poi_name': 'Mobilposta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'facebook': 'https://www.facebook.com/MagyarPosta/', 'youtube': 'https://www.youtube.com/user/magyarpostaofficial'}",
                 'poi_url_base': 'https://www.posta.hu'}]
        return data

    def process(self):
        xml = save_downloaded_xml('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        root = etree.fromstring(xml)
        for e in root.findall('post'):
            if e.find('ServicePointType').text == 'PM':
                name = 'Posta'
                code = 'hupostapo'
            elif e.find('ServicePointType').text == 'CS':
                name = 'Posta csomagautomata'
                code = 'hupostacso'
            elif e.find('ServicePointType').text == 'PP':
                name = 'PostaPont'
                code = 'hupostapp'
            else:
                logging.error('Non existing Posta type.')
            postcode = e.get('zipCode')
            street_tmp_1 = e.find('street/name').text.strip() if e.find('street/name').text is not None else None
            street_tmp_2 = e.find('street/type').text.strip() if e.find('street/type').text is not None else None
            if street_tmp_1 is None:
                street = None
            elif street_tmp_2 is None:
                street = street_tmp_1
            elif street_tmp_1 is not None and street_tmp_2 is not None:
                street = '{} {}'.format(street_tmp_1, street_tmp_2)
            else:
                logging.error('Non handled state!')
            housenumber = e.find('street/houseNumber').text.strip().lower() if e.find(
                'street/houseNumber').text is not None else None
            conscriptionnumber = None
            city = clean_city(e.find('city').text)
            branch = e.find('name').text if e.find('name').text is not None else None
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
            lunch_break_start = None
            lunch_break_stop = None
            day = e.findall('workingHours/days') if e.findall('workingHours/days') is not None else None
            oh_table = []
            for d in day:
                if len(d) != 0:
                    dict_key = dict_search(DAYS, d[0].text)
                    if len(d) == 5:
                        # Avoid duplicated values of opening and close
                        if d[1].text != d[2].text and d[3].text != d[4].text:
                            oh_table.append([dict_key, d[1].text, d[4].text, d[2].text, d[3].text])
                        else:
                            oh_table.append([dict_key, d[1].text, d[2].text, None, None])
                    elif len(d) == 3:
                        oh_table.append([dict_key, d[1].text, d[2].text, None, None])
                    else:
                        logging.warning('Errorous state.')
            if oh_table is not None:
                nonstop_num = 0
                try:
                    if oh_table[0][3] is not None and oh_table[0][4] is not None:
                        lunch_break_start = oh_table[0][3].replace('-', ':')
                        lunch_break_stop = oh_table[0][4].replace('-', ':')
                    if oh_table[0] is not None:
                        mo_o = oh_table[0][1].replace('-', ':')
                        mo_c = oh_table[0][2].replace('-', ':')
                        if '0:00' in oh_table[0][1] and (oh_table[0][2] in ['0:00', '23:59', '24:00']):
                            nonstop_num += 1
                    if oh_table[1] is not None:
                        tu_o = oh_table[1][1].replace('-', ':')
                        tu_c = oh_table[1][2].replace('-', ':')
                        if '0:00' in oh_table[1][1] and (oh_table[1][2] in ['0:00', '23:59', '24:00']):
                            nonstop_num += 1
                    if oh_table[2] is not None:
                        we_o = oh_table[2][1].replace('-', ':')
                        we_c = oh_table[2][2].replace('-', ':')
                        if '0:00' in oh_table[2][1] and (oh_table[2][2] in ['0:00', '23:59', '24:00']):
                            nonstop_num += 1
                    if oh_table[3] is not None:
                        th_o = oh_table[3][1].replace('-', ':')
                        th_c = oh_table[3][2].replace('-', ':')
                        if '0:00' in oh_table[3][1] and (oh_table[3][2] in ['0:00', '23:59', '24:00']):
                            nonstop_num += 1
                    if oh_table[4] is not None:
                        fr_o = oh_table[4][1].replace('-', ':')
                        fr_c = oh_table[4][2].replace('-', ':')
                        if '0:00' in oh_table[4][1] and (oh_table[4][2] in ['0:00', '23:59', '24:00']):
                            nonstop_num += 1
                    if oh_table[5] is not None:
                        sa_o = oh_table[5][1].replace('-', ':')
                        sa_c = oh_table[5][2].replace('-', ':')
                        if '0:00' in oh_table[5][1] and (oh_table[5][2] in ['0:00', '23:59', '24:00']):
                            nonstop_num += 1
                    if oh_table[6] is not None:
                        su_o = oh_table[6][1].replace('-', ':')
                        su_c = oh_table[6][2].replace('-', ':')
                        if '0:00' in oh_table[6][1] and (oh_table[6][2] in ['0:00', '23:59', '24:00']):
                            nonstop_num += 1
                except IndexError:
                    pass
            if nonstop_num == 7:
                nonstop = True
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
            t = OpeningHours(nonstop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c, fr_c, sa_c,
                             su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o, summer_sa_o,
                             summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c, summer_fr_c, summer_sa_c,
                             summer_su_c, lunch_break_start, lunch_break_stop)
            opening_hours = t.process()
            lat, lon = check_hu_boundary(e.find('gpsData/WGSLat').text.replace(',', '.'),
                                         e.find('gpsData/WGSLon').text.replace(',', '.'))
            geom = check_geom(lat, lon)
            original = None
            ref = None
            phone = clean_phone(e.find('phoneArea').text) if e.find('phoneArea') is not None else None
            email = e.find('email').text.strip() if e.find('email') is not None else None
            insert_data.append(
                [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                 ref, phone, email, geom, nonstop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c,
                 fr_c, sa_c, su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o, summer_sa_o,
                 summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c,
                 summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunch_break_stop, opening_hours])
        if len(insert_data) < 1:
            logging.warning('Resultset is empty. Skipping ...')
        else:
            df = pd.DataFrame(insert_data)
            df.columns = POI_COLS
            insert_poi_dataframe(self.session, df)
