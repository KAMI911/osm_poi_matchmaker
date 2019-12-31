# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from sys import exit
    import os
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)

class hu_mol(DataProvider):


    def constains(self):
        self.link = 'hhttp://trafikok.nemzetidohany.hu/mind.jsonp'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hunemdoto', 'poi_name': 'Nemzeti dohánybolt', 'poi_type': 'tobacco',
                 'poi_tags': "{'shop': 'tobacco'}",
                 'poi_url_base': 'https://www.nemzetidohany.hu/', 'poi_search_name': '(nemzeti dohánybolt|dohánybolt)', 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 0}]
        return self.__types


    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), POST_DATA)
        if soup is not None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                self.data.name = 'Nemzeti Dohánybolt'
                self.data.code = 'hunemdotob'
                self.data.postcode = poi_data['postcode'].strip()
                self.data.city = clean_city(poi_data['city'])
                self.data.original = poi_data['address']
                self.data.lat, self.data.lon = check_hu_boundary(poi_data['lat'], poi_data['lng'])
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                            self.data.postcode)
                self.data.public_holiday_open = False
                self.data.add()
