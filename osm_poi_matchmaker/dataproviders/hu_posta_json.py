# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset

except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_POSTA = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=posta'
POI_CSEKK = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
POI_CSOMAG = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postamachine',
POI_POSTA_PONT = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postapoint',


class hu_posta_json():

    def __init__(self, session, link, download_cache, filename='hu_posta.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hupostapo', 'poi_name': 'Posta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'addr:country': 'HU'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostacse', 'poi_name': 'Posta csekkautomata', 'poi_type': 'vending_machine',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'cheques', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'addr:country': 'HU'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostacso', 'poi_name': 'Posta csomagautomata', 'poi_type': 'vending_machine',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.' 'addr:country': 'HU'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostapp', 'poi_name': 'PostaPont', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.' 'addr:country': 'HU'}",
                 'poi_url_base': 'https://www.posta.hu'},
                {'poi_code': 'hupostamp', 'poi_name': 'Mobilposta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.' 'addr:country': 'HU'}",
                 'poi_url_base': 'https://www.posta.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text['items']:
                if poi_data['type'] == 'posta':
                    if 'mobilposta' in poi_data['name']:
                        data.name = 'Mobilposta'
                        data.code = 'hupostamp'
                    else:
                        data.name = 'Posta'
                        data.code = 'hupostapo'
                elif poi_data['type'] == 'csekkautomata':
                    data.name = 'Posta csekkautomata'
                    data.code = 'hupostacse'
                elif poi_data['type'] == 'postamachine':
                    data.name = 'Posta csomagautomata'
                    data.code = 'hupostacso'
                elif poi_data['type'] == 'postapoint':
                    data.name = 'PostaPont'
                    data.code = 'hupostapp'
                else:
                    logging.error('Non existing Posta type.')
                data.postcode = poi_data['zipCode'].strip()
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                data.city = clean_city(poi_data['city'])
                data.branch = poi_data['name']
                data.lat = poi_data['lat']
                data.lon = poi_data['lng']
                data.original = poi_data['address']
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
