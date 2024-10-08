# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    import traceback
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.poi_dataset import POIDatasetRaw
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_kh_bank(DataProvider):

    def __init__(self, session, download_cache, prefer_osm_postcode, link, name):
        super().__init__(session, download_cache)
        self.session = session
        self.download_cache = download_cache
        self.link = link
        self.tags = {'brand': 'K&H', 'operator': 'K&H Bank Zrt.',
                     'operator:addr': '1095 Budapest, Lechner Ödön fasor 9.', 'bic': 'OKHBHUHB',
                     'ref:vatin': 'HU10195664', 'ref:HU:vatin': '10195664-4-44', 'ref:HU:company': '01-10-041043', }
        self.prefer_osm_postcode = prefer_osm_postcode
        self.name = name
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hukhbank = {'amenity': 'bank', 'atm': 'yes',
                    'air_conditioning': 'yes', }
        hukhbank.update(self.tags)
        hukhatm = {'amenity': 'atm'}
        hukhatm.update(self.tags)
        self.__types = [
            {'poi_code': 'hukhbank', 'poi_common_name': 'K&H Bank', 'poi_type': 'bank',
             'poi_tags': hukhbank, 'poi_url_base': 'https://www.kh.hu',
             'poi_search_name': '(kh bank|k&h bank|k&h|kh)', 'osm_search_distance_perfect': 2000,
             'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 4},
            {'poi_code': 'hukhatm', 'poi_common_name': 'K&H Bank ATM', 'poi_type': 'atm',
             'poi_tags': hukhatm, 'poi_url_base': 'https://www.kh.hu',
             'poi_search_name': '(kh bank atm|k&h bank atm|k&h atm|kh atm)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 80,
             'osm_search_distance_unsafe': 3},
        ]
        return self.__types

    def process(self):
        try:
            if self.link:
                with open(self.link, 'r') as f:
                    text = json.load(f)
                    data = POIDatasetRaw()
                    for poi_data in text.get('results'):
                        first_element = next(iter(poi_data))
                        if self.name == 'K&H Bank':
                            data.code = 'hukhbank'
                            data.public_holiday_open = False
                        elif self.name == 'K&H Bank ATM':
                            data.code = 'hukhatm'
                            data.public_holiday_open = True
                        if data.code == 'hukhatm':
                            data.nonstop = True
                        else:
                            data.nonstop = False
                        data.lat, data.lon = check_hu_boundary(poi_data.get(first_element)['latitude'],
                                                               poi_data.get(first_element)['longitude'])
                        if clean_string(poi_data.get(first_element)['address']) is not None:
                            data.postcode, data.city, data.street, data.housenumber, data.conscriptionnumber = \
                                extract_all_address_waxeye(
                                    poi_data.get(first_element)['address'])
                            data.original = clean_string(poi_data.get(first_element)['address'])
                        data.phone = clean_phone_to_str(poi_data.get('phoneNumber'))
                        data.add()
                    if data is None or data.length() < 1:
                        logging.warning('Resultset is empty. Skipping ...')
                    else:
                        insert_poi_dataframe(self.session, data.process())
        except Exception as e:
            logging.exception('Exception occurred')

            logging.error(e)
            logging.error(poi_data)
