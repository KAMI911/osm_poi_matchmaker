# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'https://www.aldi.hu/hu/informaciok/informaciok/uezletkereso-es-nyitvatartas/'
POI_COMMON_TAGS = "'operator': 'ALDI Magyarország Élelmiszer Bt.', 'brand': 'Aldi', 'ref:vatin:hu':'22234663-2-44', 'brand:wikipedia':'hu:Aldi', 'brand:wikidata':'Q125054', 'addr:country': 'HU', 'facebook': 'https://www.facebook.com/ALDI.Magyarorszag', 'youtube':'https://www.youtube.com/user/ALDIMagyarorszag', 'instagram':'https://www.instagram.com/aldi.magyarorszag/', 'payment:mastercard': 'yes', 'payment:visa': 'yes'}"

class hu_aldi():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_aldi.html'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hualdisup', 'poi_name': 'Aldi', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.aldi.hu', 'poi_search_name': 'aldi'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        poi_dataset = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            table = soup.find('table', attrs={'class': 'contenttable is-header-top'})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            data = POIDataset()
            for row in rows:
                cols = row.find_all('td')
                cols = [element.text.strip() for element in cols]
                poi_dataset.append(cols)
            for poi_data in poi_dataset:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(poi_data[2])
                data.name = 'Aldi'
                data.code = 'hualdisup'
                data.postcode = poi_data[0].strip()
                data.city = clean_city(poi_data[1])
                data.original = poi_data[2]
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
