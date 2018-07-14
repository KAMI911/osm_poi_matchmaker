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
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_city, clean_javascript_variable, clean_phone, \
        clean_email
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'https://www.avia.hu/kapcsolat/toltoallomasok'


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
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Avia', 'operator': 'AVIA Hungária Kft.', 'addr:country': 'HU', 'payment:mastercard': 'yes', 'payment:visa': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes', 'email': 'avia@avia.hu', 'facebook': 'https://www.facebook.com/AVIAHungary', 'youtube': 'https://www.youtube.com/channel/UCjvjkjf2RgmKBuTnKSXk-Rg', }",
                 'poi_url_base': 'https://www.avia.hu', 'poi_search_name': 'avia'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('var\s*markers\s*=\s*((.*\n)*\]\;)', re.MULTILINE)
            script = soup.find('script', text=pattern)
            m = pattern.search(script.get_text())
            data = m.group(0)
            data = data.replace("'", '"')
            data = clean_javascript_variable(data, 'markers')
            text = json.loads(data)
            data = POIDataset()
            for poi_data in text:
                if poi_data['cim'] is not None and poi_data['cim'] != '':
                    data.postcode, data.city, data.street, data.housenumber, data.conscriptionnumber = extract_all_address(poi_data['cim'])
                data.name = 'Avia'
                data.code = 'huaviafu'
                if data.city is None:
                    data.city = poi_data['title']
                data.ref = poi_data['kutid'] if poi_data['kutid'] is not None and poi_data['kutid'] != '' else None
                data.lat, data.lon = check_hu_boundary(poi_data['lat'], poi_data['lng'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                data.website = '/toltoallomas/?id={}'.format(str(poi_data['kutid'])) if poi_data['kutid'] is not None and \
                                                                                   poi_data['kutid'] != '' else None
                data.original = poi_data['cim']
                if 'tel' in poi_data and poi_data['tel'] != '':
                    data.phone = clean_phone(poi_data['tel'])
                else:
                    data.phone = None
                if 'email' in poi_data and poi_data['email'] != '':
                    data.email = clean_email(poi_data['email'])
                else:
                    data.email = None
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
