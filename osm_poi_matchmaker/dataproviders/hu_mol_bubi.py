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
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'https://bubi.nextbike.net/maps/nextbike-live.xml?&domains=mb'


class hu_mol_bubi():
    # Processing https://bubi.nextbike.net/maps/nextbike-live.xml?&domains=mb file
    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_mol_bubi.xml'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hububibir', 'poi_name': 'MOL Bubi', 'poi_type': 'bicycle_rental',
                 'poi_tags': "{'amenity': 'bicycle_rental', 'brand': 'MOL Bubi', 'operator': 'BKK MOL Bubi', 'network': 'bubi', 'payment:credit_cards': 'yes'}",
                 'poi_url_base': 'https://molbubi.bkk.hu/'}]
        return data

    def process(self):
        xml = save_downloaded_xml('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        root = etree.fromstring(xml)
        for e in root.iter('place'):
            name = 'MOL Bubi'
            code = 'hububibir'
            housenumber = None
            conscriptionnumber = None
            street = None
            city = 'Budapest'
            branch = e.attrib['name'].split('-')[1].strip() if e.attrib['name'] is not None else None
            ref = e.attrib['name'].split('-')[0].strip() if e.attrib['name'] is not None else None
            capacity = e.attrib['bike_racks'].strip() if e.attrib['bike_racks'] is not None else None
            website = None
            nonstop = True
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
            lat, lon = check_hu_boundary(e.attrib['lat'].replace(',', '.'), e.attrib['lng'].replace(',', '.'))
            geom = check_geom(lat, lon)
            postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, None)
            original = None
            ref = None
            phone = None
            email = None
            insert_data.append(
                [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                 ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c,
                 fr_c, sa_c, su_c, summer_mo_o, summer_th_o, summer_we_o, summer_tu_o, summer_fr_o, summer_sa_o,
                 summer_su_o, summer_mo_c, summer_th_c, summer_we_c, summer_tu_c,
                 summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunck_break_stop, opening_hours])
        print(insert_data)
        if len(insert_data) < 1:
            logging.warning('Resultset is empty. Skipping ...')
        else:
            df = pd.DataFrame(insert_data)
            df.columns = POI_COLS
            insert_poi_dataframe(self.session, df)
