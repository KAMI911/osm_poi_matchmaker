# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import extract_all_address
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset

except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_cib_bank():

    def __init__(self, session, link, download_cache, prefer_osm_postcode, name):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.name = name

    @staticmethod
    def types():
        data = [{'poi_code': 'hucibbank', 'poi_name': 'CIB bank', 'poi_type': 'bank',
                 'poi_tags': "{'amenity': 'bank', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.', bic': 'CIBHHUHB', 'atm': 'yes'}",
                 'poi_url_base': 'https://www.cib.hu'},
                {'poi_code': 'hucibatm', 'poi_name': 'CIB', 'poi_type': 'atm',
                 'poi_tags': "{'amenity': 'atm', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.'}",
                 'poi_url_base': 'https://www.cib.hu'}]
        return data

    def process(self):
        if self.link:
            with open(self.link, 'r') as f:
                insert_data = []
                text = json.load(f)
                data = POIDataset()
                for poi_data in text['results']:
                    first_element = next(iter(poi_data))
                    if self.name == 'CIB bank':
                        data.name = 'CIB bank'
                        data.code = 'hucibbank'
                    else:
                        data.name = 'CIB'
                        data.code = 'hucibatm'
                    data.postcode, data.city, data.street, data.housenumber, data.conscriptionnumber = extract_all_address(
                        poi_data[first_element]['address'])
                    data.lat, data.lon = check_hu_boundary(poi_data[first_element]['latitude'],
                                                 poi_data[first_element]['longitude'])
                    data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                    data.original = poi_data[first_element]['address']
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
