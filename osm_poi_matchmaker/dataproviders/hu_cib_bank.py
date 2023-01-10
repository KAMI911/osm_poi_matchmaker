# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    import traceback
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import clean_city, clean_phone_to_str, clean_email, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.poi_dataset import POIDatasetRaw
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_cib_bank(DataProvider):

    def __init__(self, download_cache, prefer_osm_postcode, link, name):
        self.download_cache = download_cache
        self.link = link
        self.tags = {'brand': 'CIB Bank', 'operator': 'CIB Bank Zrt.', 'operator:addr': '1027 Budapest, Medve u 4-14.',
                     'bic': 'CIBHHUHB', 'ref:vatin': 'HU10136915', 'ref:HU:vatin': '10136915-4-44',
                     'ref:HU:company': '01-10-041004', 'brand:wikidata': 'Q839566',
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
            {'poi_code': 'hucibbank', 'poi_common_name': 'CIB Bank', 'poi_type': 'bank', 'poi_tags': hucibbank,
             'poi_url_base': 'https://www.cib.hu', 'poi_search_name': '(cib bank|cib)',
             'poi_search_avoid_name': '(raiffeisen|otp|k&h|budapest)',
             'osm_search_distance_perfect': 300, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'hucibatm', 'poi_common_name': 'CIB Bank ATM', 'poi_type': 'atm', 'poi_tags': hucibatm,
             'poi_url_base': 'https://www.cib.hu', 'poi_search_name': '(cib bank atm|cib atm)',
             'poi_search_avoid_name': '(raiffeisen|otp|k&h|budapest)',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10}
        ]
        return data

    def process(self):
        try:
            if self.link:
                with open(self.link, 'r') as f:
                    text = json.load(f)
                    data = POIDatasetRaw()
                    for poi_data in text['availableLocations']:
                        try:
                            if 'locationStatus' in poi_data and poi_data['locationStatus'] == 'IN_SERVICE':
                                if self.name == 'CIB Bank':
                                    data.code = 'hucibbank'
                                    data.public_holiday_open = False
                                else:
                                    data.code = 'hucibatm'
                                    data.public_holiday_open = True
                                data.lat, data.lon = check_hu_boundary(poi_data['location']['lat'],
                                                                    poi_data['location']['lon'])
                                data.city = clean_city(poi_data.get('city'))
                                data.postcode = clean_string(poi_data.get('zip'))
                                data.housenumber = clean_string(poi_data.get('streetNo'))
                                data.street = clean_string(poi_data.get('streetName'))
                                data.branch = clean_string(poi_data.get('name'))
                                data.phone = clean_phone_to_str(poi_data.get('phone'))
                                data.email = clean_email(poi_data.get('email'))
                                data.original = clean_string(poi_data.get('fullAddress'))
                                data.add()
                        except Exception as e:
                            logging.exception('Exception occurred: {}'.format(e))
                            logging.exception(traceback.print_exc())
                            logging.exception(poi_data)
                if data is None or data.length() < 1:
                    logging.warning('Resultset is empty. Skipping ...')
                else:
                    insert_poi_dataframe(data.process())
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
