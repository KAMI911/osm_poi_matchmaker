# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_phone
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_kh_bank():

    def __init__(self, session, download_cache, prefer_osm_postcode, link, name):
        self.session = session
        self.download_cache = download_cache
        self.link = link
        self.prefer_osm_postcode = prefer_osm_postcode
        self.name = name

    @staticmethod
    def types():
        data = [{'poi_code': 'hukhbank', 'poi_name': 'K&H Bank', 'poi_type': 'bank',
                 'poi_tags': "{'amenity': 'bank', 'brand': 'K&H', 'operator': 'K&H Bank Zrt.', 'bic': 'OKHBHUHB', 'atm': 'yes', 'addr:country': 'HU', 'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.kh.hu', 'poi_search_name': '(kh bank|k&h bank|k&h|kh)', 'osm_search_distance_safe': 80, 'osm_search_distance_unsafe': 4},
                {'poi_code': 'hukhatm', 'poi_name': 'K&H Bank ATM', 'poi_type': 'atm',
                 'poi_tags': "{'amenity': 'atm', 'brand': 'K&H', 'operator': 'K&H Bank Zrt.', 'addr:country': 'HU'}",
                 'poi_url_base': 'https://www.kh.hu', 'poi_search_name': '(kh bank atm|k&h bank atm|k&h atm|kh atm)', 'osm_search_distance_safe': 60, 'osm_search_distance_unsafe': 3}]
        return data

    def process(self):
        if self.link:
            with open(self.link, 'r') as f:
                text = json.load(f)
                data = POIDataset()
                for poi_data in text['results']:
                    first_element = next(iter(poi_data))
                    if self.name == 'K&H bank':
                        data.name = 'K&H bank'
                        data.code = 'hukhbank'
                        data.public_holiday_open = False
                    else:
                        data.name = 'K&H'
                        data.code = 'hukhatm'
                        data.public_holiday_open = True
                    if data.code == 'hukhatm':
                        data.nonstop = True
                    else:
                        data.nonstop = False
                    data.lat, data.lon = check_hu_boundary(poi_data[first_element]['latitude'],
                                                           poi_data[first_element]['longitude'])
                    data.postcode, data.city, data.street, data.housenumber, data.conscriptionnumber = extract_all_address(
                        poi_data[first_element]['address'])
                    data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat,
                                                                data.lon, data.postcode)
                    data.original = poi_data[first_element]['address']
                    if 'phoneNumber' in poi_data and poi_data['phoneNumber'] != '':
                        data.phone = clean_phone(poi_data['phoneNumber'])
                    else:
                        data.phone = None
                    data.add()
                if data.lenght() < 1:
                    logging.warning('Resultset is empty. Skipping ...')
                else:
                    insert_poi_dataframe(self.session, data.process())
