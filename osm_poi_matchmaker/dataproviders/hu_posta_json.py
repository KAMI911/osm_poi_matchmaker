# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.poi_dataset import POIDatasetRaw
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POI_POSTA = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=posta'
POI_CSEKK = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata',
POI_CSOMAG = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postamachine',
POI_POSTA_PONT = 'https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postapoint',


class hu_posta_json(DataProvider):

    def __init__(self, session, link, download_cache, filename='hu_posta.json'):
        self.session = session
        self.link = link
        self.tags = {'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.', 'ref:HU:vatin': '10901232-2-44',
                     'ref:vatin': 'HU10901232', 'brand:wikipedia': 'hu:Magyar Posta Zrt.', 'brand:wikidata': 'Q145614',
                     'contact:email': 'ugyfelszolgalat@posta.hu', 'contact:phone': '+36 1 767 8200',
                     'contact:facebook': 'https://www.facebook.com/MagyarPosta',
                     'contact:youtube': 'https://www.youtube.com/user/magyarpostaofficial',
                     'contact:instagram': 'https://www.instagram.com/magyar_posta_zrt/',
                     'payment:cash': 'yes', 'payment:debit_cards': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)
        self.download_cache = download_cache
        self.filename = filename

    def types(self):
        hupostapo = {'amenity': 'post_office'}
        hupostapo.update(self.tags)
        hupostacse = {'amenity': 'vending_machine', 'vending': 'cheques'}
        hupostacse.update(self.tags)
        hupostacso = {'amenity': 'parcel_locker', 'parcel_mail_in': 'yes'}
        hupostacso.update(self.tags)
        hupostapp = {'amenity': 'post_office'}
        hupostapp.update(self.tags)
        hupostamp = {'amenity': 'post_office'}
        hupostamp.update(self.tags)

        data = [
            {'poi_code': 'hupostapo', 'poi_common_name': 'Posta', 'poi_type': 'post_office',
             'poi_tags': hupostapo, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta'},
            {'poi_code': 'hupostacse', 'poi_common_name': 'Posta csekkautomata', 'poi_type': 'vending_machine_cheques',
             'poi_tags': hupostacse, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta'},
            {'poi_code': 'hupostacso', 'poi_common_name': 'Posta csomagautomata',
             'poi_type': 'vending_machine_parcel_locker',
             'poi_tags': hupostacso, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(mpl|posta)'},
            {'poi_code': 'hupostapp', 'poi_common_name': 'PostaPont', 'poi_type': 'post_office',
             'poi_tags': hupostapp, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(postapont|posta)'},
            {'poi_code': 'hupostamp', 'poi_common_name': 'Mobilposta', 'poi_type': 'post_office',
             'poi_tags': hupostamp, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                    self.filetype)
        if soup is not None:
            text = json.loads(soup)
            data = POIDatasetRaw()
            for poi_data in text['items']:
                if poi_data['type'] == 'posta':
                    if 'mobilposta' in poi_data['name']:
                        data.code = 'hupostamp'
                    else:
                        data.code = 'hupostapo'
                        data.public_holiday_open = False
                elif poi_data['type'] == 'csekkautomata':
                    data.code = 'hupostacse'
                    data.public_holiday_open = True
                elif poi_data['type'] == 'postamachine':
                    data.code = 'hupostacso'
                    data.public_holiday_open = True
                elif poi_data['type'] == 'postapoint':
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
            if data is None or data.length() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
