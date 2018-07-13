# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours, \
        clean_phone
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'http://webgispu.wigeogis.com/kunden/omvpetrom/backend/getFsForCountry.php'

POST_DATA = {'BRAND': 'OMV', 'CTRISO': 'HUN', 'MODE': 'NEXTDOOR', 'QRY': '|'}


class hu_omv():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_omv.json'):
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
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text['results']:
                data.name = 'OMV'
                data.code = 'huomvfu'
                data.postcode = poi_data['postcode'].strip()
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address_l'])
                data.city = clean_city(poi_data['town_l'])
                if poi_data['open_hours'] is not None:
                    oho, ohc = clean_opening_hours(poi_data['open_hours'])
                    if oho == '00:00' and ohc == '24:00':
                        nonstop = True
                        oho, ohc = None, None
                else:
                    oho, ohc = None, None
                for i in range(0, 7):
                    data.day_open(i, oho)
                    data.day_close(i, ohc)
                data.original = poi_data['address_l']
                data.lat, data.lon = check_hu_boundary(poi_data['y'], poi_data['x'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                if 'telnr' in poi_data and poi_data['telnr'] != '':
                    data.phone = clean_phone(poi_data['telnr'])
                else:
                    data.phone = None
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
