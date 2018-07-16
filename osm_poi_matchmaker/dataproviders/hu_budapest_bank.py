# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import json
    import os
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_phone
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


POI_DATA = 'https://www.budapestbank.hu/info/fiokkereso/process/get_data.php?action=get_data_json'

class hu_budapest_bank():
    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_budapest_bank.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename


    @staticmethod
    def types():
        data = [{'poi_code': 'hubpbank', 'poi_name': 'Budapest Bank', 'poi_type': 'bank',
                 'poi_tags': "{'amenity': 'bank', 'brand': 'Budapest Bank', 'operator': 'Budapest Bank Zrt.', 'bic': 'BUDAHUHB', 'atm': 'yes', 'addr:country': 'HU'}",
                 'poi_url_base': 'https://www.budapestbank.hu', 'poi_search_name': '(budapest bank|bp bank)'},
                {'poi_code': 'hubpatm', 'poi_name': 'Budapest Bank ATM', 'poi_type': 'atm',
                 'poi_tags': "{'amenity': 'atm', 'brand': 'Budapest Bank', 'operator': 'Budapest Bank Zrt.', 'addr:country': 'HU',}",
                 'poi_url_base': 'https://www.budapestbank.hu', 'poi_search_name': '(budapest bank|bp bank)'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text['points']:
                if poi_data['fiok'] == 1:
                    data.name = 'Budapest Bank'
                    data.code = 'hubpbank'
                else:
                    data.name = 'Budapest Bank ATM'
                    data.code = 'hubpatm'
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['addr'])
                data.postcode = poi_data['zip']
                data.city = poi_data['city_only']
                if data.code == 'hubpatm':
                    data.nonstop = True
                else:
                    data.nonstop = False
                data.lat, data.lon = check_hu_boundary(poi_data['latitude'], poi_data['longitude'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                data.original = poi_data['address']
                data.branch = poi_data['name']
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
