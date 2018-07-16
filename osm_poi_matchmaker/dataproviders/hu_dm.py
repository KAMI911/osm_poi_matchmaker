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
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'https://www.dm.hu/cms/restws/stores/find?requestingCountry=HU&countryCodes=DE%2CAT%2CBA%2CBG%2CSK%2CRS%2CHR%2CCZ%2CRO%2CSI%2CHU%2CMK%2CIT&mandantId=870&bounds=46.599301%2C17.325265%7C47.71978%2C21.681344&before=false&after=false&morningHour=9&eveningHour=18&_=1527413070144'

class hu_dm():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_dm.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hudmche', 'poi_name': 'dm', 'poi_type': 'chemist',
                 'poi_tags': "{'shop': 'chemist', 'operator': 'dm Kft.', 'brand':'dm', 'addr:country': 'HU', 'facebook':'https://www.facebook.com/dm.Magyarorszag', 'youtube': 'https://www.youtube.com/user/dmMagyarorszag', 'instagram':'https://www.instagram.com/dm_magyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.dm.hu', 'poi_search_name': 'dm'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text:
                data.name = 'dm'
                data.code = 'hudmche'
                data.postcode = poi_data['address']['plz'].strip()
                street_tmp = poi_data['address']['street'].split(',')[0]
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(street_tmp.title())
                data.city = clean_city(poi_data['address']['city'])
                data.original = poi_data['address']['street']
                data.lat, data.lon = check_hu_boundary(poi_data['location'][0], poi_data['location'][1])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                if 'telnr' in poi_data and poi_data['phone'] != '':
                    data.phone = clean_phone(poi_data['phone'])
                else:
                    data.phone = None
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
