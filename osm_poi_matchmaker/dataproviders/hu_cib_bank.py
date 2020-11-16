# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import clean_city, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_cib_bank(DataProvider):

    def __init__(self, session, download_cache, prefer_osm_postcode, link, name):
        self.session = session
        self.download_cache = download_cache
        self.link = link
        self.tags = {'brand': 'CIB Bank', 'operator': 'CIB Bank Zrt.', 'operator:addr': '1027 Budapest, Medve u 4-14.',
                     'bic': 'CIBHHUHB', 'ref:vatin': 'HU10136915', 'ref:vatin:hu': '10136915-4-44',
                     'ref:HU:company': '01 10 041004', 'brand:wikidata': 'Q839566',
                     'brand:wikipedia': 'hu:CIB Bank', }
        self.prefer_osm_postcode = prefer_osm_postcode
        self.name = name
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hucibbank = {'amenity': 'bank', 'atm': 'yes',
                     'air_conditioning': 'yes', }
        hucibbank.update(self.tags)
        hucibatm = {'amenity': 'atm'}
        hucibatm.update(self.tags)
        data = [
            {'poi_code': 'hucibbank', 'poi_name': 'CIB Bank', 'poi_type': 'bank', 'poi_tags': hucibbank,
             'poi_url_base': 'https://www.cib.hu', 'poi_search_name': '(cib bank|cib)',
             'poi_search_avoid_name': '(raiffeisen|otp|k&h)',
             'osm_search_distance_perfect': 300, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'hucibatm', 'poi_name': 'CIB Bank ATM', 'poi_type': 'atm', 'poi_tags': hucibatm,
             'poi_url_base': 'https://www.cib.hu', 'poi_search_name': '(cib bank atm|cib atm)',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10}
        ]
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
                            data.postcode = poi_data.get('zip').strip()
                            data.housenumber = poi_data['streetNo'].strip()
                            data.street = poi_data['streetName'].strip()
                            data.branch = poi_data['name']
                            if 'phone' in poi_data and poi_data['phone'] != '':
                                data.phone = clean_phone_to_str(
                                    poi_data['phone'])
                            if 'email' in poi_data and poi_data['email'] != '':
                                data.email = poi_data['email'].strip()
                            data.original = poi_data['fullAddress']
                            data.add()
                if data is None or data.lenght() < 1:
                    logging.warning('Resultset is empty. Skipping ...')
                else:
                    insert_poi_dataframe(self.session, data.process())
        except Exception as e:
            logging.exception('Exception occurred')

            logging.error(e)
            logging.error(poi_data)
