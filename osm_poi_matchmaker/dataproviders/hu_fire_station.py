# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_string, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.enums import WeekDaysLong
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# The source's "type" field (not "category" - no such field exists) carries the
# station abbreviation directly as a string, mapped here to (poi_code, full Hungarian name):
# HTP = Hivatásos Tűzoltó-parancsnokság (main_station)
# KvŐ = Katasztrófavédelmi Őrs (local_station)
# ÖTE = Önkéntes Tűzoltó Egyesület (voluntary)
# ÖTP = Önkormányzati Tűzoltó-parancsnokság (municipal)
# LTP = Létesítményi Tűzoltó-parancsnokság (concern) - documented, but never seen in the feed
# KvK = Katasztrófavédelmi Kirendeltség (district_office)
# MKI = Megyei Katasztrófavédelmi Igazgatóság (county_directorate)
# OKF = Országos Katasztrófavédelmi Főigazgatóság (national_directorate)
FIRE_STATION_TYPE_CODES = {
    'HTP': ('hufiremsta', 'Hivatásos Tűzoltó-parancsnokság'),
    'KvŐ': ('hufirelsta', 'Katasztrófavédelmi Őrs'),
    'ÖTE': ('hufirevsta', 'Önkéntes Tűzoltó Egyesület'),
    'ÖTP': ('hufireusta', 'Önkormányzati Tűzoltó-parancsnokság'),
    'LTP': ('hufirecsta', 'Létesítményi Tűzoltó-parancsnokság'),
    'KvK': ('hufirekvk', 'Katasztrófavédelmi Kirendeltség'),
    'MKI': ('hufiremki', 'Megyei Katasztrófavédelmi Igazgatóság'),
    'OKF': ('hufireokf', 'Országos Katasztrófavédelmi Főigazgatóság'),
}


class hu_fire_station(DataProvider):
    """Imports professional fire station locations in Hungary, scraped from the National Directorate for Disaster Management site."""

    def contains(self):
        self.link = 'https://www.katasztrofavedelem.hu/33856/tuzoltosagok-elhelyezkedese'
        self.tags = {'amenity': 'fire_station'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hufiremsta = self.tags.copy()
        hufiremsta.update({'fire_station:type': 'main_station'})
        hufirelsta = self.tags.copy()
        hufirelsta.update({'fire_station:type': 'local_station'})
        hufirevsta = self.tags.copy()
        hufirevsta.update({'fire_station:type': 'voluntary'})
        hufireusta = self.tags.copy()
        hufireusta.update({'fire_station:type': 'municipal'})
        hufirecsta = self.tags.copy()
        hufirecsta.update({'fire_station:type': 'concern'})
        hufirekvk = self.tags.copy()
        hufirekvk.update({'fire_station:type': 'district_office'})
        hufiremki = self.tags.copy()
        hufiremki.update({'fire_station:type': 'county_directorate'})
        hufireokf = self.tags.copy()
        hufireokf.update({'fire_station:type': 'national_directorate'})
        self.__types = [
            {'poi_code': 'hufiremsta', 'poi_common_name': 'Hivatásos Tűzoltó-parancsnokság',
             'poi_type': 'fire_station',
             'poi_tags': hufiremsta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(tűzoltó-parancsnokság)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufirelsta', 'poi_common_name': 'Katasztrófavédelmi Őrs',
             'poi_type': 'fire_station',
             'poi_tags': hufirelsta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(katasztrófavédelmi|örs)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufirevsta', 'poi_common_name': 'Önkéntes Tűzoltó Egyesület',
             'poi_type': 'fire_station',
             'poi_tags': hufirevsta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(tűzoltó|egyesület)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufireusta', 'poi_common_name': 'Önkormányzati Tűzoltó-parancsnokság',
             'poi_type': 'fire_station',
             'poi_tags': hufireusta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(tűzoltó|önkormányzati)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufirecsta', 'poi_common_name': 'Létesítményi Tűzoltó-parancsnokság',
             'poi_type': 'fire_station',
             'poi_tags': hufirecsta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(tűzoltó|létesítményi)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufirekvk', 'poi_common_name': 'Katasztrófavédelmi Kirendeltség',
             'poi_type': 'fire_station',
             'poi_tags': hufirekvk, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(katasztrófavédelmi|kirendeltség)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufiremki', 'poi_common_name': 'Megyei Katasztrófavédelmi Igazgatóság',
             'poi_type': 'fire_station',
             'poi_tags': hufiremki, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(katasztrófavédelmi|igazgatóság)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufireokf', 'poi_common_name': 'Országos Katasztrófavédelmi Főigazgatóság',
             'poi_type': 'fire_station',
             'poi_tags': hufireokf, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(katasztrófavédelmi|főigazgatóság)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100,
             'osm_search_distance_unsafe': 100},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype, False, None, self.verify_link)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                try:
                    # Extract JSON from scripts, search 'office_Map_data' among script sections
                    for section in range(len(soup.findAll('script'))):
                          if 'office_Map_data' in soup.findAll('script')[section].text:
                                pois_script = soup.findAll('script')[section].text
                                break
                    pois_variable = re.search('(\[.*\]);', pois_script)
                    pois = json.loads(pois_variable[1])
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(pois)
                if pois is None:
                    return None
                for poi_data in pois:
                    try:
                        station_type = clean_string(poi_data.get('type'))
                        if station_type not in FIRE_STATION_TYPE_CODES:
                            logging.warning('Unknown fire station type: {}'.format(poi_data.get('type')))
                            continue
                        self.data.code, full_name = FIRE_STATION_TYPE_CODES[station_type]
                        try:
                            self.data.name = poi_data.get('name').replace(station_type, full_name)
                        except Exception as err:
                            continue
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('latitude'),
                                                                         poi_data.get('longitude'))
                        self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                            self.data.conscriptionnumber = extract_all_address_waxeye(poi_data.get('address'))
                        self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                        self.data.email = clean_string(poi_data.get('email'))
                        self.data.original = clean_string(poi_data.get('address'))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
