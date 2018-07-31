# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe, search_for_postcode
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_city_street_housenumber_address, clean_city
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'http://tommarket.hu/shops'

PATTERN_TOM_MARKET = re.compile("title: '(.*)'")


class hu_tom_market():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_tom_market.html'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [
            {'poi_code': 'hutommacon', 'poi_name': 'Tom Market', 'poi_type': 'shop',
             'poi_tags': "{'shop': 'convenience', 'brand': 'Tom Market', 'addr:country': 'HU', 'payment:debit_cards': 'yes'}",
             'poi_url_base': 'https://www.tommarket.hu', 'poi_search_name': 'tom market'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            poi_data = soup.find_all('script', text=re.compile('var\s*marker'))
            poi_data_match = PATTERN_TOM_MARKET.findall(str(poi_data))
            data = POIDataset()
            for poi_data in poi_data_match:
                # if poi_data_match is not None else None
                data.city, data.street, data.housenumber, data.conscriptionnumber = extract_city_street_housenumber_address(
                    poi_data)
                data.city = clean_city(data.city)
                data.postcode = None
                if data.postcode is None:
                    data.postcode = search_for_postcode(self.session, data.city)
                data.name = 'Tom Market'
                data.code = 'hutommacon'
                data.branch = None
                data.website = None
                data.original = poi_data
                data.ref = None
                data.geom = None
                data.phone = None
                data.email = None
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                # insert_poi_dataframe(self.session, data.process())
                pass
