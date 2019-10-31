# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    from dao.data_handlers import insert_poi_dataframe
    from libs.soup import save_downloaded_soup
    from libs.address import extract_street_housenumber_better_2, clean_city
    from libs.poi_dataset import POIDataset
    from utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)

POI_POSTA = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=posta'
POI_CSEKK = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
POI_CSOMAG = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postamachine',
POI_POSTA_PONT = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postapoint',
POI_COMMON_TAGS = "'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'ref:vatin:hu' '10901232-2-44', 'ref:vatin': 'HU10901232', 'brand:wikipedia': 'hu:Magyar Posta Zrt.', 'brand:wikidata': 'Q145614',  'contact:email': 'ugyfelszolgalat@posta.hu', 'phone': '+3617678200', 'contact:facebook': 'https://www.facebook.com/MagyarPosta', 'contact:youtube': 'https://www.youtube.com/user/magyarpostaofficial', 'contact:instagram': 'https://www.instagram.com/magyar_posta_zrt/', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'"


class hu_posta_json():

    def __init__(self, session, link, download_cache, filename='hu_posta.json'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hupostapo', 'poi_name': 'Posta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta'},
                {'poi_code': 'hupostacse', 'poi_name': 'Posta csekkautomata', 'poi_type': 'vending_machine_cheques',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'cheques', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta'},
                {'poi_code': 'hupostacso', 'poi_name': 'Posta csomagautomata',
                 'poi_type': 'vending_machine_parcel_pickup',
                 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(mpl|posta)'},
                {'poi_code': 'hupostapp', 'poi_name': 'PostaPont', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(postapont|posta)'},
                {'poi_code': 'hupostamp', 'poi_name': 'Mobilposta', 'poi_type': 'post_office',
                 'poi_tags': "{'amenity': 'post_office', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta'}]
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
                        data.public_holiday_open = False
                elif poi_data['type'] == 'csekkautomata':
                    data.name = 'Posta csekkautomata'
                    data.code = 'hupostacse'
                    data.public_holiday_open = True
                elif poi_data['type'] == 'postamachine':
                    data.name = 'Posta csomagautomata'
                    data.code = 'hupostacso'
                    data.public_holiday_open = True
                elif poi_data['type'] == 'postapoint':
                    data.name = 'PostaPont'
                    data.code = 'hupostapp'
                    data.public_holiday_open = False
                else:
                    logging.error('Non existing Posta type.')
                data.postcode = poi_data['zipCode'].strip()
                data.city = clean_city(poi_data['city'])
                data.branch = poi_data['name']
                data.lat = poi_data['lat']
                data.lon = poi_data['lng']
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                data.original = poi_data['address']
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
