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

POI_DATA = 'https://benu.hu/wordpress-core/wp-admin/admin-ajax.php?action=asl_load_stores&nonce=1900018ba1&load_all=1&layout=1'


class hu_benu():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_benu.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hubenupha', 'poi_name': 'Benu gyógyszertár', 'poi_type': 'pharmacy',
                 'poi_tags': "{'amenity': 'pharmacy', 'dispensing': 'yes', 'addr:country': 'HU', 'payment:mastercard': 'yes', 'payment:visa': 'yes', 'facebook':'https://www.facebook.com/BENUgyogyszertar', 'youtube': 'https://www.youtube.com/channel/UCBLjL10QMtRHdkak0h9exqg'}",
                 'poi_url_base': 'https://www.benu.hu', 'poi_search_name': '(benu gyogyszertár|benu)'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text:
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['street'])
                if 'BENU Gyógyszertár' not in poi_data['title']:
                    data.name = poi_data['title'].strip()
                    data.branch = None
                else:
                    data.name = 'Benu gyógyszertár'
                    data.branch = poi_data['title'].strip()
                data.code = 'hubenupha'
                data.website = poi_data['description'].strip() if poi_data['description'] is not None else None
                data.website = data.website[19:]
                data.city = clean_city(poi_data['city'])
                data.postcode = poi_data['postal_code'].strip()
                data.lat, data.lon = check_hu_boundary(poi_data['lat'], poi_data['lng'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                data.original = poi_data['street']
                if 'phone' in poi_data and poi_data['phone'] != '':
                    data.phone = clean_phone(poi_data['phone'])
                else:
                    data.phone = None
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
