# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from sys import exit
    import os
    import re
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_javascript_variable, clean_opening_hours
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.enums import WeekDaysLong
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


class hu_rossmann(DataProvider):

    def constains(self):
        self.link = 'https://www.rossmann.hu/uzletkereso'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'html'

    def types(self):
        self.__types = [{'poi_code': 'hurossmche', 'poi_name': 'Rossmann', 'poi_type': 'chemist',
                 'poi_tags': "{'shop': 'chemist', 'operator': 'Rossmann Magyarország Kft.', 'operator:addr': '2225 Üllő, Zsaróka út 8.', 'ref:vatin:hu': '11149769-2-44', 'ref:vatin': 'HU11149769', 'brand':'Rossmann',  'brand:wikidata': 'Q316004', 'brand:wikipedia': 'de:Dirk Rossmann GmbH', 'contact:email': 'ugyfelszolgalat@rossmann.hu', 'phone': '+36 29 889-800;+36 70 4692 800', 'contact:facebook':'https://www.facebook.com/Rossmann.hu', 'contact:youtube': 'https://www.youtube.com/channel/UCmUCPmvMLL3IaXRBtx7-J7Q', 'contact:instagram':'https://www.instagram.com/rossmann_hu', " + POS_HU_GEN + PAY_CASH + "'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.rossmann.hu', 'poi_search_name': 'rossmann', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 3}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), None,
                                        self.verify_link)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                pattern = re.compile('^\s*var\s*places.*')
                script = soup.find('script', text=pattern)
                m = pattern.match(script.get_text())
                data = m.group(0)
                data = clean_javascript_variable(data, 'places')
                text = json.loads(data)
                for poi_data in text:
                    poi_data = poi_data['addresses'][0]
                    # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                    self.data.name = 'Rossmann'
                    self.data.code = 'hurossmche'
                    self.data.city = clean_city(poi_data['city'])
                    self.data.postcode = poi_data['zip'].strip()
                    for i in range(0, 7):
                        if poi_data['business_hours'][WeekDaysLong(i).name.lower()] is not None:
                            opening, closing = clean_opening_hours(poi_data['business_hours'][WeekDaysLong(i).name.lower()])
                            self.data.day_open_close(i, opening, closing)
                        else:
                            self.data.day_open_close(i, None, None)
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['position'][0], poi_data['position'][1])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data['address'])
                    self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat,
                                                                     self.data.lon,
                                                                     self.data.postcode)
                    self.data.original = poi_data['address']
                    self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
