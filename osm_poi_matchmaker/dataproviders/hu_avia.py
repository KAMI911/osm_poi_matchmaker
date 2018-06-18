# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe, search_for_postcode
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_city, clean_javascript_variable, clean_phone, clean_email
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'https://www.avia.hu/kapcsolat/toltoallomasok/'


class hu_avia():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_avia.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'huaviafu', 'poi_name': 'Avia', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Avia', 'operator': 'AVIA Hungária Kft.', 'payment:mastercard': 'yes', 'payment:visa': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes', 'email': 'avia@avia.hu', 'facebook': 'https://www.facebook.com/AVIAHungary', 'youtube': 'https://www.youtube.com/channel/UCjvjkjf2RgmKBuTnKSXk-Rg', }",
                 'poi_url_base': 'https://www.avia.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        insert_data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('var\s*markers\s*=\s*((.*\n)*\]\;)', re.MULTILINE)
            script = soup.find('script', text=pattern)
            m = pattern.search(script.get_text())
            data = m.group(0)
            data = data.replace("'", '"')
            data = clean_javascript_variable(data, 'markers')
            text = json.loads(data)
            for poi_data in text:
                if poi_data['cim'] is not None and poi_data['cim'] != '':
                    postcode, city, street, housenumber, conscriptionnumber = extract_all_address(poi_data['cim'])
                name = 'Avia'
                code = 'huaviafu'
                branch = None
                if city is None:
                    city = poi_data['title']
                ref = poi_data['kutid'] if poi_data['kutid'] is not None and poi_data['kutid'] != '' else None
                lat, lon = check_hu_boundary( poi_data['lat'], poi_data['lng'])
                geom = check_geom(lat, lon)
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, lat, lon, postcode)
                website = '/toltoallomas/?id={}'.format(str(poi_data['kutid'])) if poi_data['kutid'] is not None and poi_data['kutid'] != '' else None
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
                original = poi_data['cim']
                if 'tel' in poi_data and poi_data['tel'] != '':
                    phone = clean_phone( poi_data['tel'])
                else:
                    phone = None
                if 'email' in poi_data and poi_data['email'] != '':
                    email =  clean_email(poi_data['email'])
                else:
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
