# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        extract_javascript_variable, clean_opening_hours, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.enums import WeekDaysLong
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_rossmann(DataProvider):

    def constains(self):
        self.link = 'https://www.rossmann.hu/uzletkereso'
        self.tags = {'shop': 'chemist', 'operator': 'Rossmann Magyarország Kft.',
                     'operator:addr': '2225 Üllő, Zsaróka út 8.', 'ref:vatin:hu': '11149769-2-44',
                     'ref:vatin': 'HU11149769', 'brand': 'Rossmann', 'brand:wikidata': 'Q316004',
                     'brand:wikipedia': 'de:Dirk Rossmann GmbH', 'contact:email': 'ugyfelszolgalat@rossmann.hu',
                     'phone': '+36 29 889-800;+36 70 4692 800',
                     'contact:facebook': 'https://www.facebook.com/Rossmann.hu',
                     'contact:youtube': 'https://www.youtube.com/channel/UCmUCPmvMLL3IaXRBtx7-J7Q',
                     'contact:instagram': 'https://www.instagram.com/rossmann_hu', 'air_conditioning': 'yes'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hurossmche = self.tags.copy()
        hurossmche.update(POS_HU_GEN)
        hurossmche.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hurossmche', 'poi_name': 'Rossmann', 'poi_type': 'chemist',
             'poi_tags': hurossmche, 'poi_url_base': 'https://www.rossmann.hu', 'poi_search_name': 'rossmann',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 3}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype, None, self.verify_link)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                text = json.loads(extract_javascript_variable(soup, 'locations'))
                pois = []
                for poi_temp in text:
                    try:
                        poi_id = int(clean_string(poi_temp.get('id')))
                        pois.insert(poi_id, poi_temp)
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_temp)
                text = json.loads(extract_javascript_variable(soup, 'additionals'))
                for poi_temp in text:
                    try:
                        poi_id = int(clean_string(poi_temp.get('id')))
                        pois.insert(poi_id, poi_temp)
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_temp)
                for poi_data in pois:
                    try:
                        # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                        self.data.name = 'Rossmann'
                        self.data.code = 'hurossmche'
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.postcode = clean_string(poi_data.get('zip')) \
                            if ('zip' in poi_data and poi_data.get('zip') != '') else None
                        for i in range(0, 7):
                            if WeekDaysLong(i).name.lower() in poi_data:
                                opening, closing = clean_opening_hours(
                                    poi_data[WeekDaysLong(i).name.lower()])
                                self.data.day_open_close(i, opening, closing)
                            else:
                                self.data.day_open_close(i, None, None)
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(poi_data.get('street'))
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
