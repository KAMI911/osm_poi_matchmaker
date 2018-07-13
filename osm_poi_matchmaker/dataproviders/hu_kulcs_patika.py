# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'http://kulcspatika.hu/inc/getPagerContent.php?tipus=patika&kepnelkul=true&latitude=47.498&longitude=19.0399'
POST_DATA = {'kepnelkul': 'true', 'latitude': '47.498', 'longitude': '19.0399', 'tipus': 'patika'}


class hu_kulcs_patika():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_kulcs_patika.json'):
        self.session = session
        self.link = 'https://kulcspatika.hu/inc/getPagerContent.php?tipus=patika&kepnelkul=true&latitude=47.498&longitude=19.0399'
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hukulcspha', 'poi_name': 'Kulcs patika', 'poi_type': 'pharmacy',
                 'poi_tags': "{'amenity': 'pharmacy', 'dispensing': 'yes', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.kulcspatika.hu'}]
        return data

    def process(self):
        if self.link:
            with open(self.link, 'r') as f:
                text = json.load(f)
            for poi_data in text:
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['cim'])
                if 'Kulcs patika' not in poi_data['nev']:
                    data.name = poi_data['nev'].strip()
                    data.branch = None
                else:
                    data.name = 'Kulcs patika'
                    data.branch = poi_data['nev'].strip()
                data.code = 'hukulcspha'
                data.website = poi_data['link'].strip() if poi_data['link'] is not None else None
                data.city = clean_city(poi_data['helyseg'])
                data.lat, data.lon = check_hu_boundary(poi_data['marker_position']['latitude'],
                                             poi_data['marker_position']['longitude'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon,
                                                       poi_data['irsz'].strip())
                data.original = poi_data['cim']
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
