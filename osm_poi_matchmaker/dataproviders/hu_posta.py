# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    from lxml import etree
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.xml import save_downloaded_xml
    from osm_poi_matchmaker.libs.address import clean_city, clean_phone, clean_street, clean_street_type
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.enums import WeekDaysLongHU
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/PostInfo.xml'
POI_COMMON_TAGS = "'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'operator:addr': '1138 Budapest, Dunavirág utca 2-6.', 'ref:vatin:hu': '10901232-2-44', 'ref:vatin': 'HU10901232', 'brand:wikipedia': 'hu:Magyar Posta Zrt.', 'brand:wikidata': 'Q145614', 'addr:country': 'HU', 'email': 'ugyfelszolgalat@posta.hu', 'phone': '+3617678200', 'facebook': 'https://www.facebook.com/MagyarPosta', 'youtube': 'https://www.youtube.com/user/magyarpostaofficial', 'instagram': 'https://www.instagram.com/magyar_posta_zrt', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'"


class hu_posta(DataProvider):
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
                 'poi_tags': "{'amenity': 'post_office', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta', 'osm_search_distance_safe': 200},
                {'poi_code': 'hupostacse', 'poi_name': 'Posta csekkautomata', 'poi_type': 'vending_machine_cheques',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'cheques', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta', 'osm_search_distance_safe': 200},
                {'poi_code': 'hupostacso', 'poi_name': 'Posta csomagautomata',
                 'poi_type': 'vending_machine_parcel_pickup',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(mpl|posta)', 'osm_search_distance_safe': 200},
                {'poi_code': 'hupostapp', 'poi_name': 'PostaPont', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(postapont|posta)', 'osm_search_distance_safe': 200},
                {'poi_code': 'hupostamp', 'poi_name': 'Mobilposta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta', 'osm_search_distance_safe': 200}]
        return data

    def process(self):
        xml = save_downloaded_xml('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        root = etree.fromstring(xml)
        data = POIDataset()
        for e in root.findall('post'):
            #  The 'kirendeltség' post offices are not available to end users, so we remove them
            if 'okmányiroda' in e.find('name').text.lower() or 'mol kirendeltség' in e.find('name').text.lower():
                logging.debug('Skipping non public post office.')
                continue
            else:
                if e.find('ServicePointType').text == 'PM':
                    data.name = 'Posta'
                    data.code = 'hupostapo'
                    data.public_holiday_open = False
                elif e.find('ServicePointType').text == 'CS':
                    data.name = 'Posta csomagautomata'
                    data.code = 'hupostacso'
                    data.public_holiday_open = True
                elif e.find('ServicePointType').text == 'PP':
                    data.name = 'PostaPont'
                    data.code = 'hupostapp'
                    data.public_holiday_open = False
                else:
                    logging.error('Non existing Posta type.')
                data.postcode = e.get('zipCode')
                data.housenumber = e.find('street/houseNumber').text.strip().lower() if e.find(
                    'street/houseNumber').text is not None else None
                data.conscriptionnumber = None
                data.city = clean_city(e.find('city').text)
                data.branch = e.find('name').text if e.find('name').text is not None else None
                day = e.findall('workingHours/days') if e.findall('workingHours/days') is not None else None
                oh_table = []
                for d in day:
                    if len(d) != 0:
                        day_key = WeekDaysLongHU[d[0].text].value
                        if len(d) == 5:
                            # Avoid duplicated values of opening and close
                            if d[1].text != d[3].text and d[2].text != d[4].text:
                                oh_table.append([day_key, d[1].text, d[4].text, d[2].text, d[3].text])
                            else:
                                logging.warning(
                                    'Dulicated opening hours in post office: {}: {}-{}; {}-{}.'.format(data.branch,
                                                                                                       d[1].text,
                                                                                                       d[2].text,
                                                                                                       d[3].text,
                                                                                                       d[4].text))
                                oh_table.append([day_key, d[1].text, d[2].text, None, None])
                        elif len(d) == 3:
                            oh_table.append([day_key, d[1].text, d[2].text, None, None])
                        else:
                            logging.warning('Errorous state.')
                nonstop_num = 0
                if oh_table is not None:
                    try:
                        # Set luch break if there is a lunch break on Monday
                        if oh_table[0][3] is not None and oh_table[0][4] is not None:
                            data.lunch_break_start = oh_table[0][3].replace('-', ':')
                            data.lunch_break_stop = oh_table[0][4].replace('-', ':')
                        for i in range(0, 7):
                            if oh_table[i] is not None:
                                data.day_open(i, oh_table[i][1].replace('-', ':'))
                                data.day_close(i, oh_table[i][2].replace('-', ':'))
                                if '0:00' in oh_table[i][1] and (oh_table[i][2] in ['0:00', '23:59', '24:00']):
                                    nonstop_num += 1
                    except IndexError:
                        pass
                if nonstop_num == 7:
                    data.nonstop = True
                data.lat, data.lon = check_hu_boundary(e.find('gpsData/WGSLat').text.replace(',', '.'),
                                                       e.find('gpsData/WGSLon').text.replace(',', '.'))
                street_tmp_1 = clean_street(e.find('street/name').text.strip()) if e.find(
                    'street/name').text is not None else None
                street_tmp_2 = clean_street_type(e.find('street/type').text.strip()) if e.find(
                    'street/type').text is not None else None
                if street_tmp_2 is None:
                    data.street = street_tmp_1
                elif street_tmp_1 is not None and street_tmp_2 is not None:
                    data.street = '{} {}'.format(street_tmp_1, street_tmp_2)
                else:
                    logging.error('Non handled state!')
                data.phone = clean_phone(e.find('phoneArea').text) if e.find('phoneArea') is not None else None
                data.email = e.find('email').text.strip() if e.find('email') is not None else None
                data.add()
        if data.lenght() < 1:
            logging.warning('Resultset is empty. Skipping ...')
        else:
            insert_poi_dataframe(self.session, data.process())
