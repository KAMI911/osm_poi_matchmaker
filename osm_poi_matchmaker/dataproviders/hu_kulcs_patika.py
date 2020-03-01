# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


POST_DATA = {'kepnelkul': 'true', 'latitude': '47.498', 'longitude': '19.0399', 'tipus': 'patika'}


class hu_kulcs_patika(DataProvider):


    def constains(self):
        self.link = 'https://kulcspatika.hu/inc/getPagerContent.php?tipus=patika&kepnelkul=true&latitude=47.498&longitude=19.0399'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'json'
        self.headers = {'Referer': 'https://kulcspatika.hu/patikak', 'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0', 'Accept': 'application/json, text/javascript, */*; q=0.01'}

    def types(self):
        self.__types = [{'poi_code': 'hukulcspha', 'poi_name': 'Kulcs Patika', 'poi_type': 'pharmacy',
                 'poi_tags': "{'amenity': 'pharmacy', 'brand': 'Kulcs Patika', 'dispensing': 'yes', " + POS_HU_GEN + PAY_CASH + "'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.kulcspatika.hu/', 'poi_search_name': '(kulcs patika|kulcs)', 'preserve_original_name': True}]
        return self.__types

    def process(self):
        try:
            if self.link:
                '''
                soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), None,
                                            self.verify_link, headers=self.headers)
                if soup is not None:
                text = json.loads(soup.get_text())
                '''
                with open(os.path.join(self.download_cache, self.filename), 'r') as f:
                    text = json.load(f)
                    for poi_data in text:
                        if 'Kulcs patika' not in poi_data['nev']:
                            self.data.name = poi_data['nev'].strip()
                            self.data.branch = None
                        else:
                            self.data.name = 'Kulcs patika'
                            self.data.branch = poi_data['nev'].strip()
                        self.data.code = 'hukulcspha'
                        self.data.website = poi_data['link'].strip() if poi_data['link'] is not None else None
                        self.data.city = clean_city(poi_data['helyseg'])
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data['marker_position']['latitude'],
                                                               poi_data['marker_position']['longitude'])
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            poi_data['cim'])
                        self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                                    poi_data['irsz'].strip())
                        self.data.original = poi_data['cim']
                        self.data.public_holiday_open = False
                        self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
