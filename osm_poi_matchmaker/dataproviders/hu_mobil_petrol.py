# -*- coding: utf-8 -*-

try:
    from builtins import Exception, ImportError, range, isinstance
    import traceback
    import logging
    from sys import exit
    import json
    import os
    import re
    from dao.data_handlers import insert_poi_dataframe
    from libs.soup import save_downloaded_soup
    from libs.address import clean_city, extract_street_housenumber_better_2, clean_phone_to_str, clean_javascript_variable
    from libs.geo import check_hu_boundary
    from libs.osm import query_postcode_osm_external
    from libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from libs.poi_dataset import POIDataset
    from utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


class hu_mobil_petrol(DataProvider):


    def constains(self):
        self.link = 'http://www.mpetrol.hu/'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'humobpefu', 'poi_name': 'Mobil Petrol', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Mobil Petrol', 'contact:email': 'info@mpetrol.hu', 'contact:facebook': 'https://www.facebook.com/mpetrolofficial/', 'name': 'Mobil Petrol', 'operator:addr': '1095 Budapest, Ipar utca 2.', 'operator': 'MPH Power Zrt.', " + POS_HU_GEN + PAY_CASH + "'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}",
                 'poi_url_base': 'http://mpetrol.hu/', 'poi_search_name': '(mobil metrol|shell)'}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup != None:
                # parse the html using beautiful soap and store in variable `soup`
                pattern = re.compile('^\s*var\s*totem_stations.*')
                script = soup.find('script', text=pattern)
                m = pattern.match(script.get_text())
                data = m.group(0)
                data = clean_javascript_variable(data, 'totem_stations')
                text = json.loads(data)
                for poi_data in text.values():
                    self.data.name = 'Mobil Petrol'
                    self.data.code = 'humobpefu'
                    self.data.website = poi_data.get('description')
                    self.data.city = clean_city(poi_data.get('city'))
                    self.data.original = poi_data.get('address')
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['location']['lat'], poi_data['location']['lng'])
                    self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon, None)
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data.get('address'))
                    self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                    self.data.public_holiday_open = False
                    if '0-24' in poi_data:
                        self.data.nonstop = True
                        self.data.public_holiday_open = True
                    else:
                        if '6-22' in poi_data:
                            open_from = 6
                            open_to = 22
                        elif '6-21' in poi_data:
                            open_from = 6
                            open_to = 21
                        elif '5-22' in poi_data:
                            open_from = 5
                            open_to = 22
                        elif '6-18' in poi_data:
                            open_from = 6
                            open_to = 18
                        if 'open_from' in locals() and 'open_to' in locals():
                            for i in range(0, 7):
                                self.data.day_open(i, open_from)
                                self.data.day_close(i, open_to)
                        self.data.public_holiday_open = False
                    self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
