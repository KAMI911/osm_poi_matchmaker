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

POI_DATA = 'http://toltoallomaskereso.mol.hu/hu/portlet/routing/along_latlng.json'
POST_DATA = {'country': 'Magyarország', 'lat': '47.162494', 'lng': '19.503304100000037', 'radius': 20}


class hu_mol():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_mol.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'humolfu', 'poi_name': 'MOL', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'MOL', 'operator': 'MOL Nyrt.', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'https://www.mol.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), POST_DATA)
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text:
                data.name = 'MOL'
                data.code = 'humolfu'
                data.postcode = poi_data['postcode'].strip()
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                data.city = clean_city(poi_data['city'])
                data.original = poi_data['address']
                data.lat, data.lon = check_hu_boundary(poi_data['lat'], poi_data['lng'])
                postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
