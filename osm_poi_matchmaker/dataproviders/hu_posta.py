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
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/PostInfo.xml'


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
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostacse', 'poi_name': 'Posta csekkautomata', 'poi_type': 'vending_machine',
                 'poi_tags': "{'amenity': 'vending_machine', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.'}",
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
            housenumber = e.find('street/houseNumber').text.strip().lower() if e.find('street/houseNumber').text is not None else None
            conscriptionnumber = None
            city = clean_city(e.find('city').text)
            branch = e.find('name').text if e.find('name').text is not None else None
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
            lon = e.find('gpsData/WGSLon').text.replace(',', '.')
            lat = e.find('gpsData/WGSLat').text.replace(',', '.')
            # This is a workaround because original datasource may contains swapped lat / lon parameters
            if float(lon) < 46:
                logging.warning('Replaced coordinates. ({}, {}'.format(branch, city))
                lon, lat = lat, lon
            # Another workaround to insert missing decimal point
            if float(lon) > 200:
                logging.warning('Missing lon decimal separator. ({}, {}'.format(branch, city))
                lon = '{}.{}'.format(lon[:2], lon[3:])
            if float(lat) > 200:
                logging.warning('Missing lat decimal separator. ({}, {}'.format(branch, city))
                lat = '{}.{}'.format(lat[:2], lat[3:])
            geom = check_geom(lon, lat)
            postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
            original = None
            ref = None
            insert_data.append(
                [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                 ref, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c, fr_c, sa_c, su_c])
        if len(insert_data) < 1:
            logging.warning('Resultset is empty. Skipping ...')
        else:
            df = pd.DataFrame(insert_data)
            df.columns = POI_COLS
            insert_poi_dataframe(self.session, df)