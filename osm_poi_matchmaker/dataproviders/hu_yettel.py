# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_yettel(DataProvider):
    """Imports Yettel telecom shop locations in Hungary from the JSON markers embedded
    as a data-markers attribute on yettel.hu/elerhetoseg/uzletkereso's map container -
    the same data the page's own store-finder map reads (same embedding style as
    hu_kulcs_patika.py's pharmacy finder)."""

    def contains(self):
        self.link = 'https://www.yettel.hu/elerhetoseg/uzletkereso'
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'shop': 'mobile_phone', 'brand': 'Yettel', 'brand:wikidata': 'Q268578',
                     'contact:website': 'https://www.yettel.hu'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huyettel = self.tags.copy()
        self.__types = [
            {'poi_code': 'huyettel', 'poi_common_name': 'Yettel', 'poi_type': 'mobile_phone', 'poi_tags': huyettel,
             'poi_url_base': 'https://www.yettel.hu', 'poi_search_name': 'yettel',
             'additional_ref_name': 'ref', 'osm_search_distance_perfect': 500,
             'osm_search_distance_safe': 100, 'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, verify=self.verify_link, headers=self.headers)
            if soup is None:
                return
            map_div = soup.find('div', id='outlet-map')
            if map_div is None or map_div.get('data-markers') is None:
                logging.warning('Could not find Yettel store markers on the page.')
                return
            markers = json.loads(map_div.get('data-markers'), strict=False)
            for poi_data in markers or []:
                try:
                    self.data.code = 'huyettel'
                    self.data.ref = clean_string(poi_data.get('code') or poi_data.get('id'))
                    name = clean_string(poi_data.get('displayname')) or ''
                    self.data.branch = name.removeprefix('Yettel ') or None
                    self.data.city = clean_city(poi_data.get('city'))
                    self.data.postcode = clean_string(poi_data.get('zipcode'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(poi_data.get('address'))
                    self.data.original = clean_string(poi_data.get('address'))
                    self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lon'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
