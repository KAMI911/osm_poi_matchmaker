# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_aldi(DataProvider):

    def constains(self):
        self.link = 'https://www.aldi.hu/uzletek/'
        self.tags = {'operator': 'ALDI Magyarország Élelmiszer Bt.',
                     'operator:addr': '2051 Biatorbágy, Mészárosok útja 2.', 'brand': 'Aldi',
                     'ref:vatin:hu': '22234663-2-44', 'ref:vatin': 'HU22234663',
                     'ref:HU:company': '13 06 058506', 'ref:company:HU': '13-06-058506',
                     'brand:wikipedia': 'hu:Aldi', 'brand:wikidata': 'Q125054',
                     'contact:facebook': 'https://www.facebook.com/ALDI.Magyarorszag',
                     'contact:youtube': 'https://www.youtube.com/user/ALDIMagyarorszag',
                     'contact:instagram': 'https://www.instagram.com/aldi.magyarorszag',
                     'air_conditioning': 'yes', }
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hualdisup = {'shop': 'supermarket'}
        hualdisup.update(self.tags)
        self.__types = [
            {'poi_code': 'hualdisup', 'poi_name': 'Aldi', 'poi_type': 'shop', 'poi_tags': hualdisup,
             'poi_url_base': 'https://www.aldi.hu', 'poi_search_name': 'aldi'},
        ]
        return self.__types

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                    self.filetype)
        poi_dataset = []
        if soup is not None:
            # parse the html using beautiful soap and store in variable `soup`
            table = soup.find(
                'table', attrs={'class': 'contenttable is-header-top'})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [element.text.strip() for element in cols]
                poi_dataset.append(cols)
            for poi_data in poi_dataset:
                # Assign: code, postcode, city, name, branch, website, original
                #         street, housenumber, conscriptionnumber, ref, geom
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data[2])
                self.data.name = 'Aldi'
                self.data.code = 'hualdisup'
                self.data.postcode = poi_data[0]
                self.data.city = poi_data[1]
                self.data.original = clean_string(poi_data[2])
                self.data.public_holiday_open = False
                self.data.add()
