# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_kulcs_patika(DataProvider):

    def constains(self):
        self.link = 'https://kulcspatikak.hu/gykeress_feed.php'
        self.tags = {'amenity': 'pharmacy', 'brand': 'Kulcs Patika',
                     'dispensing': 'yes', 'air_conditioning': 'yes'}
        self.headers = {'Referer': 'https://kulcspatikak.hu/patikakereso',
                        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0',
                        'Accept': 'application/json, text/javascript, */*; q=0.01'}
        self.post = {'megyeid': -1, 'quality_check': 0,
                     'animal_check': 0, 'nonstop_check': 0, 'hetvege_check': 0}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hukulcspha = self.tags.copy()
        hukulcspha.update(POS_HU_GEN)
        hukulcspha.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hukulcspha', 'poi_name': 'Kulcs Patika', 'poi_type': 'pharmacy',
             'poi_tags': hukulcspha,
             'poi_url_base': 'https://www.kulcspatika.hu/', 'poi_search_name': '(kulcs patika|kulcs)',
             'preserve_original_name': True},
        ]
        return self.__types

    def process(self):
        try:
            if self.link:
                # soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache,
                #                            self.filename), self.post, self.verify_link, headers=self.headers)
                with open(os.path.join(self.download_cache, self.filename), 'r') as f:
                    text = json.load(f)
                    if text is not None:
                        text = json.loads(text, strict=False)
                        for poi_data in text:
                            try:
                                if 'Kulcs patika' not in poi_data.get('nev'):
                                    self.data.name = poi_data.get(
                                        'nev').strip()
                                    self.data.branch = None
                                else:
                                    self.data.name = 'Kulcs patika'
                                    self.data.branch = poi_data.get(
                                        'nev').strip()
                                self.data.code = 'hukulcspha'
                                if poi_data.get('link') is not None and poi_data.get('link') != '':
                                    self.data.website = poi_data.get('link').strip() if poi_data.get('link') \
                                        is not None else None
                                if poi_data.get('helyseg') is not None and poi_data.get('helyseg') != '':
                                    self.data.city = clean_city(
                                        poi_data.get('helyseg'))
                                self.data.lat, self.data.lon = \
                                    check_hu_boundary(poi_data.get('marker_position')['latitude'],
                                                      poi_data.get('marker_position')['longitude'])
                                if poi_data.get('cim') is not None and poi_data.get('cim') != '':
                                    self.data.original = poi_data.get('cim')
                                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                                        extract_street_housenumber_better_2(
                                            poi_data.get('cim'))
                                if poi_data.get('irsz') is not None and poi_data.get('irsz') != '':
                                    self.data.postcode = poi_data.get(
                                        'irsz').strip()
                                self.data.public_holiday_open = False
                                self.data.add()
                            except Exception as e:
                                logging.error(e)
                                logging.error(poi_data)
                                logging.exception('Exception occurred')

        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')
