# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_dm(DataProvider):


    def constains(self):
        self.link = 'https://www.dm.hu/cms/restws/stores/find?requestingCountry=HU&countryCodes=DE%2CAT%2CBA%2CBG%2CSK%2CRS%2CHR%2CCZ%2CRO%2CSI%2CHU%2CMK%2CIT&mandantId=870&bounds=46.599301%2C17.325265%7C47.71978%2C21.681344&before=false&after=false&morningHour=9&eveningHour=18&_=1527413070144'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hudmche', 'poi_name': 'dm', 'poi_type': 'chemist',
                 'poi_tags': "{'shop': 'chemist', 'operator': 'dm Kft.', 'brand':'dm', 'addr:country': 'HU', 'facebook':'https://www.facebook.com/dm.Magyarorszag', 'youtube': 'https://www.youtube.com/user/dmMagyarorszag', 'instagram':'https://www.instagram.com/dm_magyarorszag', 'payment:cash': 'yes', 'payment:contactless': 'yes', 'payment:mastercard': 'yes', 'payment:visa': 'yes', 'air_conditioning': 'yes'}",
                 'poi_url_base': 'https://www.dm.hu', 'poi_search_name': 'dm'}]
        return self.__types

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text:
                self.data.name = 'dm'
                self.data.code = 'hudmche'
                self.data.postcode = poi_data['address']['plz'].strip()
                street_tmp = poi_data['address']['street'].split(',')[0]
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                    street_tmp.title())
                self.data.city = clean_city(poi_data['address']['city'])
                self.data.original = poi_data['address']['street']
                self.data.lat, self.data.lon = check_hu_boundary(poi_data['location'][0], poi_data['location'][1])
                self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                            self.data.postcode)
                if 'telnr' in poi_data and poi_data['phone'] != '':
                    self.data.phone = clean_phone(poi_data['phone'])
                else:
                    self.data.phone = None
                self.data.public_holiday_open = False
                self.data.add()

