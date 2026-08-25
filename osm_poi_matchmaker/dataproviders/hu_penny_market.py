# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


PENNY_DAY_INDEX = {'HÉ': 0, 'KE': 1, 'SZE': 2, 'CSÜT': 3, 'PÉ': 4, 'SZO': 5, 'VAS': 6}


class hu_penny_market(DataProvider):
    """Imports Penny Market supermarket locations in Hungary from penny.hu's stores API."""

    def contains(self):
        self.link = 'https://www.penny.hu/api/stores'
        self.tags = {'shop': 'supermarket', 'operator': 'Penny Market Kft.', 'brand': 'Penny Market',
                     'brand:wikidata': 'Q284688', 'brand:wikipedia': 'en:Penny (supermarket)',
                     'internet_access': 'wlan', 'internet_access:fee': 'no', 'internet_access:ssid': 'PENNY FREE WLAN',
                     'contact:email': 'ugyfelszolgalat@penny.hu',
                     'contact:facebook': 'PennyMarketMagyarorszag',
                     'contact:instagram': 'pennymarkethu',
                     'contact:youtube': 'https://www.youtube.com/channel/UCSy0KKUrDxVWkx8qicky_pQ',
                     'ref:HU:vatin': '10969629-2-44', 'ref:vatin': 'HU10969629'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hupennysup = self.tags.copy()
        hupennysup.update(POS_HU_GEN)
        hupennysup.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hupennysup', 'poi_common_name': 'Penny Market', 'poi_type': 'shop',
             'poi_tags': hupennysup, 'poi_url_base': 'https://www.penny.hu', 'poi_search_name': 'penny',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 15},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    try:
                        self.data.code = 'hupennysup'
                        self.data.postcode = clean_string(poi_data.get('zip'))
                        street_tmp = clean_string((poi_data.get('street') or '').split(',')[0])
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.original = clean_string(poi_data.get('street'))
                        position = poi_data.get('position') or {}
                        self.data.lat, self.data.lon = check_hu_boundary(position.get('lat'), position.get('lng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(street_tmp)
                        self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                        self.data.ref = clean_string(poi_data.get('storeId'))
                        self.data.public_holiday_open = False
                        for block in poi_data.get('openingTimes') or []:
                            day_index = PENNY_DAY_INDEX.get(block.get('dayOfWeek'))
                            times = block.get('times') or []
                            if day_index is not None and len(times) >= 2:
                                self.data.day_open(day_index, times[0])
                                self.data.day_close(day_index, times[1])
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
