# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import time
    import traceback
    import json
    import urllib.parse
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup, download_content
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_city, clean_phone_to_str, \
        clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# Hungarian day abbreviations used in the store finder's "H-Szo 10:00-21:00, V
# 10:00-18:00" hours notation, mapped to Monday=0..Sunday=6.
DAY_ABBR = {'h': 0, 'k': 1, 'sze': 2, 'cs': 3, 'p': 4, 'szo': 5, 'v': 6}

# Matches one `{ name:"...", addr:"...", city:"...", phone:"...", hours:"...", q:"..." }`
# record from the page's embedded `var stores = [...]` array - see process().
STORE_RE = re.compile(
    r'name:"(?P<name>[^"]*)"\s*,\s*addr:"(?P<addr>[^"]*)"\s*,\s*city:"(?P<city>[^"]*)"\s*,\s*'
    r'phone:"(?P<phone>[^"]*)"\s*,\s*hours:"(?P<hours>[^"]*)"'
)
# Nominatim's public instance was found to hard-429 every single request from this
# project's environment regardless of rate limiting or retries (verified independently
# of this code, incl. via a plain curl from outside the container - the whole egress IP
# is blocked, not just this process). Photon (https://photon.komoot.io) is a separate
# OSM-data-backed geocoder unaffected by that block, so it's used here instead.
PHOTON_ENDPOINT = 'https://photon.komoot.io/api/'
GEOCODE_HEADERS = {'User-Agent': 'osm_poi_matchmaker/1.0 (+https://github.com/kami911/osmpoi; '
                                 'miholmikor@gmail.com)'}
GEOCODE_RETRIES = 4
GEOCODE_RETRY_SLEEP = 3
# Photon has no published hard rate limit, but a short pause between requests is a
# baseline courtesy for a shared free instance during a ~29-store harvest.
GEOCODE_RATE_LIMIT_SLEEP = 1.1

HOURS_SEGMENT_RE = re.compile(
    r'(?P<from_day>[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]+)(?:[–-](?P<to_day>[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]+))?\s+'
    r'(?P<from_time>\d{1,2}:\d{2})[–-](?P<to_time>\d{1,2}:\d{2})'
)


def _parse_hours(hours_text, data):
    """Expand a "H-Szo 10:00-21:00, V 10:00-18:00" style string into day_open()/
    day_close() calls. Any segment or day abbreviation that doesn't parse is just
    skipped (logged at debug) rather than failing the whole store.
    """
    if not hours_text:
        return
    for segment in hours_text.split(','):
        m = HOURS_SEGMENT_RE.search(segment.strip())
        if m is None:
            logging.debug('Unparsed opening-hours segment: %r', segment)
            continue
        from_day = DAY_ABBR.get(m.group('from_day').strip().lower())
        to_day = DAY_ABBR.get(m.group('to_day').strip().lower()) if m.group('to_day') else from_day
        if from_day is None or to_day is None:
            logging.debug('Unrecognised day abbreviation in opening-hours segment: %r', segment)
            continue
        for day in range(from_day, to_day + 1):
            data.day_open(day, m.group('from_time'))
            data.day_close(day, m.group('to_time'))


def _geocode(addr):
    """Resolve a free-text address to (latitude, longitude) via the Photon API.

    Returns (None, None) if every attempt failed, or came back with no match, so
    the caller can just check `if lat is None`.
    """
    url = '{}?{}'.format(PHOTON_ENDPOINT, urllib.parse.urlencode({'q': addr, 'limit': 1}))
    for attempt in range(GEOCODE_RETRIES):
        raw = download_content(url, headers=GEOCODE_HEADERS)
        time.sleep(GEOCODE_RATE_LIMIT_SLEEP)
        if raw is None:
            logging.warning('Geocoding request failed for %r (attempt %d/%d), retrying.',
                           addr, attempt + 1, GEOCODE_RETRIES)
            time.sleep(GEOCODE_RETRY_SLEEP)
            continue
        try:
            features = json.loads(raw).get('features', [])
        except ValueError as e:
            logging.warning('Malformed geocoding response for %r: %s', addr, e)
            return None, None
        if not features:
            return None, None
        lon, lat = features[0]['geometry']['coordinates']
        return lat, lon
    return None, None


class hu_yves_rocher(DataProvider):
    """Imports Yves Rocher cosmetics store locations in Hungary from the store list
    embedded as a `var stores = [...]` JS array in the store finder page
    https://www.yves-rocher.hu/uzletek. The previous source, an API on
    storelocator.yves-rocher.eu, is gone - that subdomain no longer resolves at all.

    The page gives a free-text address but no coordinates and no other endpoint on
    the site carries any (checked the full page for a store-locator AJAX call, a
    map widget or JSON-LD coordinates - there is none, only this static list), so
    each store is geocoded via Photon (see _geocode()). Nominatim was tried first
    since OSMPythonTools already vendors it and it's used elsewhere in the project
    for area lookups, but its public instance hard-429s every request from this
    project's environment regardless of rate limiting or retries.
    """

    def contains(self):
        self.link = 'https://www.yves-rocher.hu/uzletek'
        self.tags = {'shop': 'cosmetics', 'operator': 'Yves Rocher Hungary Kft. ',
                     'brand': 'Yves Rocher', 'brand:wikidata': 'Q28496595',
                     'brand:wikipedia': 'en:Yves Rocher (company)', 'contact:email': 'vevoszolgalat@yrnet.com',
                     'contact:facebook': 'YvesRocherHungary',
                     'contact:youtube': 'https://www.youtube.com/channel/UC6GA7lucPWgbNlC_MoomB9g',
                     'contact:instagram': 'yves_rocher_magyarorszag',
                     'operator:addr': '1132 Budapest, Váci út 20-26.', 'ref:vatin': 'HU10618646',
                     'ref:HU:vatin': '10618646-2-41', 'ref:HU:company': '01-09-079930', 'air_conditioning': 'yes'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        huyvesrcos = self.tags.copy()
        huyvesrcos.update(POS_HU_GEN)
        huyvesrcos.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'huyvesrcos', 'poi_common_name': 'Yves Rocher', 'poi_type': 'cosmetics',
             'poi_tags': huyvesrcos, 'poi_url_base': 'https://www.yves-rocher.hu/',
             'poi_search_name': 'yves rocher',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 15},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is None:
                return
            script = soup.find('script', string=re.compile('var stores'))
            if script is None:
                logging.warning('Could not find the "var stores" script block on the page.')
                return
            for poi_data in STORE_RE.finditer(script.string):
                try:
                    self.data.code = 'huyvesrcos'
                    self.data.branch = clean_string(poi_data.group('name'))
                    addr = poi_data.group('addr')
                    self.data.original = clean_string(addr)
                    self.data.postcode, _, self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_all_address_waxeye(addr)
                    self.data.city = clean_city(poi_data.group('city'))
                    self.data.phone = clean_phone_to_str(poi_data.group('phone'))
                    lat, lon = _geocode(addr)
                    if lat is None:
                        logging.warning('No geocoding result for %r, skipping store.', addr)
                        continue
                    self.data.lat, self.data.lon = check_hu_boundary(lat, lon)
                    _parse_hours(poi_data.group('hours'), self.data)
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data.group(0))
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
