# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    from dao.data_handlers import insert_poi_dataframe
    from libs.soup import save_downloaded_soup
    from libs.address import extract_street_housenumber_better_2, clean_city
    from libs.poi_dataset import POIDataset
    from utils.data_provider import DataProvider
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


class hu_aldi(DataProvider):

    def constains(self):
        self.link = 'https://www.aldi.hu/hu/informaciok/informaciok/uezletkereso-es-nyitvatartas'
        self.POI_COMMON_TAGS = "'operator': 'ALDI Magyarország Élelmiszer Bt.', 'operator:addr': '2051 Biatorbágy, Mészárosok útja 2.', 'brand': 'Aldi', 'ref:vatin:hu':'22234663-2-44', 'ref:vatin':'HU22234663', ,'brand:wikipedia':'hu:Aldi', ,'brand:wikidata':'Q125054', 'addr:country': 'HU', 'facebook': 'https://www.facebook.com/ALDI.Magyarorszag', 'youtube':'https://www.youtube.com/user/ALDIMagyarorszag', 'instagram':'https://www.instagram.com/aldi.magyarorszag', 'payment:mastercard': 'yes', 'payment:visa': 'yes', 'air_conditioning': 'yes'}"
        self.filename = self.filename + 'html'

    def types(self):
        self.__types = [{'poi_code': 'hualdisup', 'poi_name': 'Aldi', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.aldi.hu', 'poi_search_name': 'aldi'}]
        return self.__types

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
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data[2])
                self.data.name = 'Aldi'
                self.data.code = 'hualdisup'
                self.data.postcode = poi_data[0].strip()
                self.data.city = clean_city(poi_data[1])
                self.data.original = poi_data[2]
                self.data.public_holiday_open = False
                self.data.add()
