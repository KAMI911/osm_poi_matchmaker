# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_aldi(DataProvider):

    def contains(self):
        self.link = 'https://www.aldi.hu/hu/hu/.get-stores-in-radius.json?latitude=47.162494&longitude=19.503304&radius=50000'
        self.tags = {'operator': 'ALDI Magyarország Élelmiszer Bt.',
                     'operator:addr': '2051 Biatorbágy, Mészárosok útja 2.', 'brand': 'Aldi',
                     'ref:HU:vatin': '22234663-2-44', 'ref:vatin': 'HU22234663',
                     'ref:company:HU': '13 06 058506',
                     'brand:wikipedia': 'hu:Aldi', 'brand:wikidata': 'Q125054',
                     'contact:facebook': 'https://www.facebook.com/ALDI.Magyarorszag',
                     'contact:youtube': 'https://www.youtube.com/user/ALDIMagyarorszag',
                     'contact:instagram': 'https://www.instagram.com/aldi.magyarorszag',
                     'air_conditioning': 'yes', }
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hualdisup = {'shop': 'supermarket'}
        hualdisup.update(self.tags)
        self.__types = [
            {'poi_code': 'hualdisup', 'poi_common_name': 'Aldi', 'poi_type': 'shop', 'poi_tags': hualdisup,
             'poi_url_base': 'https://www.aldi.hu', 'poi_search_name': 'aldi',
             'osm_search_distance_perfect': 1000, 'osm_search_distance_safe': 200},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            poi_dataset = []
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                text = json.loads(soup, strict=False)
                for poi_data in text.get('stores'):
                    try:
                        if poi_data.get('countryCode') == 'HU':
                            self.data.code = 'hualdisup'
                            # Assign: code, postcode, city, name, branch, website, original
                            #         street, housenumber, conscriptionnumber, ref, geom
                            self.data.city = poi_data.get('city')
                            self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('latitude'), poi_data.get('longitude'))
                            self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(poi_data.get('streetAddress'))
                            self.data.postcode = clean_string(poi_data.get('postalCode'))
                            self.data.original = clean_string(poi_data.get('streetAddress'))
                            self.data.public_holiday_open = False
                            self.data.phone = clean_string(poi_data.get('phoneNumber'))
                            for i in range(7):
                                mi = i + 1
                                for opening_day in poi_data.get('openUntilSorted').get('openingHours'):
                                    if mi > 6:
                                        mi -= 7
                                    if opening_day.get('dayIdx') == mi:
                                        self.data.day_open(i, opening_day.get('open'))
                                        self.data.day_close(i, opening_day.get('close'))
                                        break
                            '''
                            self.data.description.
                            'BAKEBOX', 'Helyben sütött pékáru'
                            'CAR_PARKING_LOT', 'Parkolóhely'
                            'COFFEE_TOGO', ''
                            'POST_STATION', 'Csomagküldő automata'
                            'GAS_STATION', 'Benzinkút'
                            'ELECTRIC_CAR_CHARGER', 'E-töltőállomás'
                            TODO: Wheelchair
                            'WHEELCHAIR_ACCESS', 'Akadálymentesített'
                            '''
                            self.data.add()
                    except Exception as e:
                        logging.error(e)
                        logging.error(poi_data)
                        logging.exception('Exception occurred')
        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')
