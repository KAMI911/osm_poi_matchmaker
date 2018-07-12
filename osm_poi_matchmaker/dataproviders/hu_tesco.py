# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_javascript_variable, clean_opening_hours_2, clean_phone
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'https://tesco.hu/aruhazak/'
POI_COMMON_TAGS = "'operator': 'TESCO-GLOBAL Áruházak Zrt.', 'ref:vatin:hu': '10307078-2-44', 'brand': 'Tesco', 'wikipedia': 'hu:Tesco', 'wikidata': 'Q487494', 'facebook': 'https://www.facebook.com/tescoaruhazak/', 'youtube': 'https://www.youtube.com/user/TescoMagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'"


class hu_tesco():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_tesco.html'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hutescoexp', 'poi_name': 'Tesco Expressz', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.tesco.hu'},
                {'poi_code': 'hutescoext', 'poi_name': 'Tesco Extra', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.tesco.hu'},
                {'poi_code': 'hutescosup', 'poi_name': 'Tesco', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.tesco.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            # script = soup.find('div', attrs={'data-stores':True})
            script = soup.find(attrs={'data-stores': True})
            text = json.loads(script['data-stores'])
            data = POIDataset()
            for poi_data in text:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                data.city = clean_city(poi_data['city'])
                data.branch = poi_data['name']
                if 'xpres' in poi_data['name']:
                    data.name = 'Tesco Expressz'
                    data.code = 'hutescoexp'
                elif 'xtra' in poi_data['name']:
                    data.name = 'Tesco Extra'
                    data.code = 'hutescoext'
                else:
                    data.name = 'Tesco'
                    data.code = 'hutescosup'
                data.website = poi_data['url']
                opening = json.loads(poi_data['opening'])
                for i in range(0, 7):
                    ind = str(i + 1) if i != 6 else '0'
                    if ind in opening:
                        data.day_open(i, opening[ind][0])
                        data.day_close(i, opening[ind][1])
                data.lat, data.lon = check_hu_boundary(poi_data['gpslat'], poi_data['gpslng'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, None)
                data.original = poi_data['address']
                if 'phone' in poi_data and poi_data['phone'] != '':
                    data.phone = clean_phone(poi_data['phone'])
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
