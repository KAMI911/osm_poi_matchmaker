# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import datetime
    import traceback
    import xml.etree.ElementTree as ElementTree
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

DAYS_EN = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
HOURS_LINE_RE = re.compile(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:-(Mon|Tue|Wed|Thu|Fri|Sat|Sun))?\s+'
                           r'(\d{1,2}:\d{2})-(\d{1,2}:\d{2})')


class hu_sinsay(DataProvider):
    """Imports Sinsay clothing store locations in Hungary from the same global KML
    feed (sinsay.com/it/it/new_kml/sinsay_stores_en_<today's date>.kml) the site's own
    store finder uses, filtered to placemarks whose description names Hungary
    ("Magyarország") as the country. The feed carries no postcode at all."""

    def contains(self):
        today = datetime.datetime.now().strftime('%Y%m%d')
        self.link = 'https://www.sinsay.com/it/it/new_kml/sinsay_stores_en_{}.kml'.format(today)
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'shop': 'clothes', 'brand': 'Sinsay', 'brand:wikidata': 'Q107410579',
                     'contact:website': 'https://www.sinsay.com'}
        # FileType.csv (not .xml) - raw passthrough, no BeautifulSoup reformatting risk;
        # this KML is parsed directly with the stdlib xml.etree below.
        self.filetype = FileType.csv
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        husinsay = self.tags.copy()
        self.__types = [
            {'poi_code': 'husinsay', 'poi_common_name': 'Sinsay', 'poi_type': 'clothes', 'poi_tags': husinsay,
             'poi_url_base': 'https://www.sinsay.com', 'poi_search_name': 'sinsay',
             'osm_search_distance_perfect': 500, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def process(self):
        try:
            raw = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                       self.filetype, False, headers=self.headers)
            if raw is None:
                return
            root = ElementTree.fromstring(re.sub(r'\sxmlns="[^"]*"', '', raw, count=1))
            for placemark in root.findall('.//Placemark'):
                try:
                    name_el = placemark.find('name')
                    desc_el = placemark.find('description')
                    coords_el = placemark.find('.//coordinates')
                    if desc_el is None or desc_el.text is None or coords_el is None or coords_el.text is None:
                        continue
                    paragraphs = re.findall(r'<p>(.*?)</p>', desc_el.text, re.DOTALL)
                    if len(paragraphs) < 3 or 'Magyarorsz' not in paragraphs[2]:
                        continue
                    self.data.code = 'husinsay'
                    name = clean_string(name_el.text if name_el is not None else None) or ''
                    self.data.branch = name.removeprefix('SINSAY').strip() or None
                    street_full = paragraphs[0].strip()
                    self.data.city = clean_city(paragraphs[1].strip())
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(street_full)
                    self.data.original = clean_string(street_full)
                    lon, lat, _ = coords_el.text.strip().split(',')
                    self.data.lat, self.data.lon = check_hu_boundary(lat, lon)
                    opening_hours = []
                    for match in HOURS_LINE_RE.finditer('\n'.join(paragraphs[4:])):
                        start_day, end_day, open_time, close_time = match.groups()
                        start_index = DAYS_EN.index(start_day)
                        end_index = DAYS_EN.index(end_day) if end_day else start_index
                        for day_index in range(start_index, end_index + 1):
                            opening_hours.append((day_index, open_time, close_time))
                    for day_index, open_time, close_time in opening_hours:
                        self.data.day_open_close(day_index, open_time, close_time)
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
