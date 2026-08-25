# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_city, clean_string, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_kulcs_patika(DataProvider):
    """Imports Kulcs Patika pharmacy locations in Hungary, scraped from kulcspatikak.hu's pharmacy finder."""

    def contains(self):
        self.link = 'https://kulcspatikak.hu/patikakereso'
        self.tags = {'amenity': 'pharmacy', 'brand': 'Kulcs Patikák',
                     'dispensing': 'yes', 'air_conditioning': 'yes'}
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hukulcspha = self.tags.copy()
        hukulcspha.update(POS_HU_GEN)
        hukulcspha.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hukulcspha', 'poi_common_name': 'Kulcs Patika', 'poi_type': 'pharmacy',
             'poi_tags': hukulcspha,
             'poi_url_base': 'https://www.kulcspatikak.hu/', 'poi_search_name': '(kulcs patikák|kulcs patika|kulcs)',
             'preserve_original_name': True},
        ]
        return self.__types

    def process(self):
        try:
            if self.link:
                soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache,
                                            self.filename), self.filetype,
                                            verify=self.verify_link, headers=self.headers)
                if soup is not None:
                    # The pharmacy list is embedded as a JSON attribute on the map container, not a
                    # separate AJAX/API call.
                    map_div = soup.find('div', class_='map')
                    if map_div is None or map_div.get('data-markers') is None:
                        logging.warning('Could not find pharmacy markers on the page.')
                        return
                    text = json.loads(map_div.get('data-markers'), strict=False)
                    logging.debug(text)
                    for poi_data in text or []:
                        try:
                            name = clean_string(poi_data.get('marker-title'))
                            if name is not None and 'Kulcs patika' not in name:
                                self.data.name = name
                                self.data.branch = None
                            else:
                                self.data.branch = name
                            self.data.code = 'hukulcspha'
                            address_part, _, phone_part = (poi_data.get('marker-desc') or '').partition('<br>')
                            phone_match = re.search(r'Tel:\s*(.+)', phone_part)
                            if phone_match is not None:
                                self.data.phone = clean_phone_to_str(phone_match.group(1))
                            self.data.lat, self.data.lon = \
                                check_hu_boundary(poi_data.get('marker-lat'),
                                                  poi_data.get('marker-lng'))
                            self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                                self.data.conscriptionnumber = extract_all_address_waxeye(address_part)
                            self.data.public_holiday_open = False
                            self.data.add()
                        except Exception as e:
                            logging.exception('Exception occurred: {}'.format(e))
                            logging.exception(traceback.format_exc())
                            logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
