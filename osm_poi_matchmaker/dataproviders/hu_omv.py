# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from sys import exit
    import os
    import json
    from dao.data_handlers import insert_poi_dataframe
    from libs.soup import save_downloaded_soup
    from libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours, \
        clean_phone_to_str
    from libs.geo import check_hu_boundary
    from libs.osm import query_postcode_osm_external
    from libs.poi_dataset import POIDataset
    from libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


POST_DATA = {'BRAND': 'OMV', 'CTRISO': 'HUN', 'MODE': 'NEXTDOOR', 'QRY': '|'}


class hu_omv(DataProvider):


    def constains(self):
        self.link = 'http://webgispu.wigeogis.com/kunden/omvpetrom/backend/getFsForCountry.php'
        self.POI_COMMON_TAGS = "'amenity': 'fuel', 'name': 'OMV', 'brand': 'OMV', 'fuel:diesel': 'yes', " \
                               "'fuel:octane_95': 'yes', 'air_conditioning': 'yes', 'brand:wikidata': 'Q168238', " \
                               "'brand:wikipedia': 'en:OMV', 'operator': 'OMV Hungária Kft.', " \
                               "'operator:addr': '1117 Budapest, Október huszonharmadika utca 6-10 5. emelet 5/A.', " \
                               "'ref:vatin:hu': '10542925-2-44', 'ref:vatin': 'HU10542925', " \
                               "'ref:HU:company': '01-09-071584', 'contact:email': 'info.hungary@omv.com', " \
                               "'contact:facebook': 'https://www.facebook.com/omvmagyarorszag', " \
                               "'contact:fax': '+36 1 381 9899', 'contact:twitter': 'omv', " \
                               "'contact:linkedin': 'https://www.linkedin.com/company/omv', " \
                               "'contact:instagram': 'https://www.instagram.com/omv/', " \
                               "'contact:youtube': 'https://www.youtube.com/user/omvofficial', " \
                               "'contact:phone': '+36 1 381 9700', 'contact:website': 'http://www.omv.hu/'}"
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'huomvfu', 'poi_name': 'OMV', 'poi_type': 'fuel',
                 'poi_tags': "{" + self.POI_COMMON_TAGS + POS_HU_GEN + PAY_CASH + "}",
                 'poi_url_base': 'https://www.omv.hu', 'poi_search_name': 'omv', 'osm_search_distance_perfect': 2000,
                 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 60}]
        return self.__types


    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), POST_DATA)
            if soup is not None:
                text = json.loads(soup.get_text())
                for poi_data in text['results']:
                    self.data.name = 'OMV'
                    self.data.code = 'huomvfu'
                    self.data.postcode = poi_data['postcode'].strip()
                    self.data.city = clean_city(poi_data['town_l'])
                    if poi_data['open_hours'] is not None:
                        oho, ohc = clean_opening_hours(poi_data['open_hours'])
                        if oho == '00:00' and ohc == '24:00':
                            self.data.nonstop = True
                            self.data.public_holiday_open = True
                            oho, ohc = None, None
                        else:
                            self.data.public_holiday_open = False
                    else:
                        oho, ohc = None, None
                        self.data.public_holiday_open = False
                    for i in range(0, 7):
                        self.data.day_open(i, oho)
                        self.data.day_close(i, ohc)
                    self.data.original = poi_data['address_l']
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['y'], poi_data['x'])
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data['address_l'])
                    self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                                self.data.postcode)
                    if 'telnr' in poi_data and poi_data['telnr'] != '':
                        self.data.phone = clean_phone_to_str(poi_data['telnr'])
                    else:
                        self.data.phone = None
                    self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
