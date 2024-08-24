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
    from osm_poi_matchmaker.libs.osm import query_osm_city_name
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_ccc(DataProvider):


    def contains(self):
        self.link = 'https://ccc.eu/hu/sklepy'
        self.tags = {'shop': 'shoes', 'operator': ' CCC Hungary Shoes Kft.',
                     'operator:addr': '1123 Budapest, Alkotás utca 53.', 'ref:HU:vatin': '24128296-2-44',
                     'ref:vatin': 'HU24128296', 'ref:HU:company': '01-09-991763', 'brand': 'CCC', 'brand:wikidata': 'Q11788344',
                     'brand:wikipedia': 'pl:CCC (przedsiębiorstwo)', 'contact:email': 'info.hu@ccc.eu',
                     'contact:phone': '+36 1 445 3701', 'contact:linkedin': 'https://www.linkedin.com/company/cccsa',
                     'contact:facebook': 'https://www.facebook.com/CCC.Hungary/',
                     'contact:youtube': 'https://www.youtube.com/channel/UCVscWDmL_2JddDdGuku7f2w',
                     'contact:instagram': 'https://www.instagram.com/cccshoesbags_hu/', 'air_conditioning': 'yes'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)


    def types(self):
        hucccsho = self.tags.copy()
        hucccsho.update(POS_HU_GEN)
        hucccsho.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hucccsho', 'poi_common_name': 'CCC', 'poi_type': 'shoes',
             'poi_tags': hucccsho, 'poi_url_base': 'https://ccc.eu/hu/', 'poi_search_name': 'ccc',
             'poi_search_avoid_name': '(deichmann)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 50},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                pois = json.loads(soup.find('div', {"id": "pos-list-json"}).text)
                for poi_data in pois:
                    try:
                        self.data.code = 'hucccsho'
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('latitude'), poi_data.get('longitude'))
                        self.data.postcode = clean_string(poi_data.get('postcode'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                                poi_data.get('street'))
                        opening_hours_raw = poi_data.get('openings')
                        self.data.original = clean_string(poi_data.get('street'))
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
