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
            lon = e.attrib['lng'].replace(',', '.')
            lat = e.attrib['lat'].replace(',', '.')
            # This is a workaround because original datasource may contains swapped lat / lon parameters
            if float(lon) < 46:
                lon, lat = lat, lon
            # Another workaround to insert missing decimal point
            if float(lon) > 200:
                lon = '{}.{}'.format(lon[:2], lon[3:])
            if float(lat) > 200:
                lat = '{}.{}'.format(lat[:2], lat[3:])
            geom = check_geom(lon, lat)
            postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, None)
            original = None
            ref = None
            phone = None
            email = None
            insert_data.append(
                [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                 ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c,
                 fr_c, sa_c, su_c])
        print(insert_data)
        if len(insert_data) < 1:
            logging.warning('Resultset is empty. Skipping ...')
        else:
            df = pd.DataFrame(insert_data)
            df.columns = POI_COLS
            insert_poi_dataframe(self.session, df)