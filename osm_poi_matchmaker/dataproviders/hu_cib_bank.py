# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_city, clean_phone_to_str, extract_street_housenumber_better_2
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.data_provider import DataProvider


except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_cib_bank():

    def __init__(self, session, download_cache, prefer_osm_postcode, link, name):
        self.session = session
        self.download_cache = download_cache
        self.link = link
        self.prefer_osm_postcode = prefer_osm_postcode
        self.name = name

    @staticmethod
    def types():
        data = [{'poi_code': 'hucibbank', 'poi_name': 'CIB Bank', 'poi_type': 'bank',
                 'poi_tags': "{'amenity': 'bank', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.', 'bic': 'CIBHHUHB', 'atm': 'yes',  'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.cib.hu', 'poi_search_name': '(cib bank|cib)'},
                {'poi_code': 'hucibatm', 'poi_name': 'CIB Bank ATM', 'poi_type': 'atm',
                 'poi_tags': "{'amenity': 'atm', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.'}",
                 'poi_url_base': 'https://www.cib.hu', 'poi_search_name': '(cib bank atm|cib atm)'}]
        return data

    def process(self):
        try:
            if self.link:
                with open(self.link, 'r') as f:
                    text = json.load(f)
                    data = POIDataset()
                    for poi_data in text['availableLocations']:
                        if 'locationStatus' in poi_data and poi_data['locationStatus'] == 'IN_SERVICE':
                            if self.name == 'CIB Bank':
                                data.name = 'CIB Bank'
                                data.code = 'hucibbank'
                                data.public_holiday_open = False
                            else:
                                data.name = 'CIB Bank ATM'
                                data.code = 'hucibatm'
                                data.public_holiday_open = True
                            data.lat, data.lon = check_hu_boundary(poi_data['location']['lat'],
                                                                   poi_data['location']['lon'])
                            data.city = clean_city(poi_data['city'])
                            data.postcode = poi_data['zip'].strip()
                            data.housenumber = poi_data['streetNo'].strip()
                            data.street = poi_data['streetName'].strip()
                            data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat,
                                                                        data.lon, data.postcode)
                            data.branch = poi_data['name']
                            if 'phone' in poi_data and poi_data['phone'] != '':
                                data.phone = clean_phone_to_str(poi_data['phone'])
                            if 'email' in poi_data and poi_data['email'] != '':
                                data.email = poi_data['email'].strip()
                            data.original = poi_data['fullAddress']
                            data.add()
                if data is None or data.lenght() < 1:
                    logging.warning('Resultset is empty. Skipping ...')
                else:
                    insert_poi_dataframe(self.session, data.process())
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)