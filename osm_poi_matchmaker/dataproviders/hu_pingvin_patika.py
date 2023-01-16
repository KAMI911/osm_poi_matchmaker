# -*- coding: utf-8 -*-

try:
    from builtins import Exception, ImportError, range
    import logging
    import sys
    import json
    import os
    import re
    import traceback
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, extract_city_street_housenumber_address, clean_phone_to_str, \
        extract_javascript_variable, clean_string, clean_url
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_pingvin_patika(DataProvider):

    def contains(self):
        self.link = 'https://pingvinpatika.hu/patikak'

        self.tags = {'brand': 'Pingvin Patika', 'dispensing': 'yes',
                     'contact:facebook': 'https://www.facebook.com/pingvinpatika/',
                     'contact:youtube': 'https://www.youtube.com/channel/UCYw0X4BHJg9ba8Xz1UGj8Vw',
                     'contact:email': 'webaruhaz@pingvinpatika.hu',
                     'contact:instagram': 'https://www.instagram.com/pingvinpatikak',
                     'contact:pinterest': 'https://www.pinterest.com/pingvinpatika/',
                     'operator': 'Pingvin Napfény Zrt.', 'operator:addr': '6720 Szeged, Széchenyi tér 17.',
                     'ref:vatin': 'HU24700450', 'ref:HU:vatin': '24700450-2-06',
                     'ref:HU:company': '06-10-000456', 'air_conditioning': 'yes', }
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hupingvpha = self.tags.copy()
        hupingvpha.update(POS_HU_GEN)
        hupingvpha.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hupingvpha', 'poi_common_name': 'Pingvin Patika', 'poi_type': 'pharmacy',
             'poi_tags': hupingvpha, 'poi_url_base': 'https://pingvinpatika.hu',
             'poi_search_name': '(pingvinpatika|pingvin patika)',
             'poi_search_avoid_name': '(alma|benu|plus|kulcs|unipatika)',
             'osm_search_distance_perfect': 1000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 20, 'preserve_original_name': True},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                extract = extract_javascript_variable(soup, 'pharmacies')
                # extract = extract[:-1]
                extract = extract.replace('">', '\">')
                extract = extract.replace('="', '=\"')
                extract = extract.replace('" ', '\" ')
                pois = json.loads(extract)
                for poi_data in pois:
                    try:
                        self.data.code = 'hupingvpha'
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lon'))
                        self.data.city, self.data.street, self.data.housenumber, \
                        self.data.conscriptionnumber = extract_city_street_housenumber_address(poi_data.get('address'))

                        # TODO: Process opening_hours
                        soup = BeautifulSoup(poi_data.get('content'))
                        try:
                            self.data.description = soup.find_all('p')[-1].text
                        except Exception as e:
                            logging.exception('Exception occurred: {}'.format(e))
                            logging.exception(traceback.print_exc())
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
