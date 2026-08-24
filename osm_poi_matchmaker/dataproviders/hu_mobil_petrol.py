# -*- coding: utf-8 -*-

try:
    from builtins import Exception, ImportError, range
    import logging
    import sys
    import html
    import os
    import re
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, extract_street_housenumber_better_2, clean_phone_to_str, \
        clean_string, clean_url
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_mobil_petrol(DataProvider):

    def contains(self):
        # The homepage no longer embeds a totem_stations JS variable; the "Super Store Finder"
        # plugin now serves the station list as this static XML feed instead (found via the
        # browser's Network tab - it isn't reachable through admin-ajax.php, which this site
        # firewalls off for every custom action).
        self.link = 'https://mpetrol.hu/wp-content/plugins/superstorefinder-wp/ssf-wp-xml.php'
        self.tags = {'amenity': 'fuel', 'brand': 'Mobil Petrol', 'contact:email': 'info@mpetrol.hu',
                     'contact:facebook': 'https://www.facebook.com/mpetrolofficial/', 'name': 'Mobil Petrol',
                     'operator:addr': '1095 Budapest, Ipar utca 2.', 'operator': 'MPH Power Zrt.', 'fuel:diesel': 'yes',
                     'fuel:octane_95': 'yes'}
        self.filetype = FileType.xml
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        humobpefu = self.tags.copy()
        humobpefu.update(POS_HU_GEN)
        humobpefu.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'humobpefu', 'poi_common_name': 'Mobil Petrol', 'poi_type': 'fuel',
             'poi_tags': humobpefu, 'poi_url_base': 'http://mpetrol.hu/',
             'poi_search_name': '(mobil metrol|shell)',
             'poi_search_avoid_name': '(mol|shell|avia|lukoil|hunoil)'}
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                for poi_data in soup.findAll('item'):
                    try:
                        self.data.code = 'humobpefu'
                        city = clean_city(poi_data.location.get_text()) if poi_data.location is not None else None
                        self.data.city = city
                        # The feed bakes street+housenumber, city and zip into one combined string,
                        # e.g. "Fő út 31.  Ajka,   8400" - split it back up using the (reliable)
                        # city name and the zip after the last comma.
                        address_raw = poi_data.address.get_text() if poi_data.address is not None else ''
                        self.data.original = clean_string(address_raw)
                        street_and_city, _, postcode = address_raw.rpartition(',')
                        self.data.postcode = clean_string(postcode)
                        street_part = street_and_city
                        if city and street_and_city.strip().endswith(city):
                            street_part = street_and_city.strip()[:-len(city)]
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(street_part)
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.latitude.get_text() if poi_data.latitude is not None else None,
                            poi_data.longitude.get_text() if poi_data.longitude is not None else None)
                        self.data.phone = clean_phone_to_str(
                            poi_data.telephone.get_text()) if poi_data.telephone is not None else None
                        website = poi_data.website.get_text() if poi_data.website is not None else None
                        self.data.website = clean_url(website) if website else None
                        self.data.public_holiday_open = False
                        description = html.unescape(poi_data.description.get_text()) \
                            if poi_data.description is not None else ''
                        if re.search(r'H-V:\s*0[:.]00\s*[-\u2013]\s*24[:.]00', description):
                            self.data.nonstop = True
                            self.data.public_holiday_open = True
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
