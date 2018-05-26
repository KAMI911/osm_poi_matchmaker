# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, clean_city, clean_opening_hours, clean_phone
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'http://webgispu.wigeogis.com/kunden/omvpetrom/backend/getFsForCountry.php'

POST_DATA = {'BRAND': 'OMV', 'CTRISO': 'HUN', 'MODE': 'NEXTDOOR', 'QRY': '|'}


class hu_omv():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_mol.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'huomvfu', 'poi_name': 'OMV', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'OMV', 'operator': 'OMV Hungária Kft.', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'https://www.omv.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), POST_DATA)
        insert_data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text['results']:
                name = 'OMV'
                code = 'huomvfu'
                postcode = poi_data['postcode'].strip()
                street, housenumber, conscriptionnumber = extract_street_housenumber_better(
                    poi_data['address_l'])
                city = clean_city(poi_data['town_l'])
                branch = None
                website = None
                nonstop = None
                if poi_data['open_hours'] is not None:
                    oho, ohc = clean_opening_hours(poi_data['open_hours'])
                    if oho == '00:00' and ohc == '24:00':
                        nonstop = True
                        oho, ohc = None, None
                else:
                    oho, ohc = None, None
                mo_o = oho
                th_o = oho
                we_o = oho
                tu_o = oho
                fr_o = oho
                sa_o = oho
                su_o = oho
                mo_c = ohc
                th_c = ohc
                we_c = ohc
                tu_c = ohc
                fr_c = ohc
                sa_c = ohc
                su_c = ohc
                original = poi_data['address_l']
                ref = None
                lat, lon = check_hu_boundary(poi_data['y'], poi_data['x'])
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                if 'telnr' in poi_data and poi_data['telnr'] != '':
                    phone = clean_phone(poi_data['telnr'])
                else:
                    phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c, fr_c, sa_c, su_c])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
