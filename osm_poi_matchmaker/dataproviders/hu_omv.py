# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours, \
        clean_phone
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

    
POST_DATA = {'BRAND': 'OMV', 'CTRISO': 'HUN', 'MODE': 'NEXTDOOR', 'QRY': '|'}


class hu_omv(DataProvider):


    def constains(self):
        self.link = 'http://webgispu.wigeogis.com/kunden/omvpetrom/backend/getFsForCountry.php'
        self.POI_COMMON_TAGS = ""

    def types(self):
        self.__types = [{'poi_code': 'huomvfu', 'poi_name': 'OMV', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'OMV', 'operator': 'OMV Hungária Kft.', 'addr:country': 'HU', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes', 'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.omv.hu', 'poi_search_name': 'omv'}]
        return self.__types


    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), POST_DATA)
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text['results']:
                self.data.name = 'OMV'
                self.data.code = 'huomvfu'
                self.data.postcode = poi_data['postcode'].strip()
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address_l'])
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
                self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                            self.data.postcode)
                if 'telnr' in poi_data and poi_data['telnr'] != '':
                    self.data.phone = clean_phone(poi_data['telnr'])
                else:
                    self.data.phone = None
                self.data.add()

