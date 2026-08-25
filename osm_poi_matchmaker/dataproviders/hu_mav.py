# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import math
    import traceback
    import pandas as pd
    from urllib.parse import urlencode
    from timeit import default_timer as timer
    from datetime import timedelta
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup, extract_zip
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_opening_hours, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.enums import WeekDaysLong
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# MAV's own GTFS feed has no UIC station code or Wikidata item, but Wikidata carries both
# (P722/UIC station code, and the item's own QID) for essentially every Hungarian railway
# station - see hu_mav.py module discussion. Matched against our GTFS stops by exact
# (suffix-stripped) name, disambiguated by nearest coordinate when a name is shared by more
# than one Wikidata item (rare - mostly distinct halts with the same generic name).
WIKIDATA_SPARQL_URL = 'https://query.wikidata.org/sparql'
WIKIDATA_SPARQL_QUERY = '''SELECT ?station ?stationLabel ?uic ?coord WHERE {
  ?station wdt:P17 wd:Q28.
  ?station wdt:P722 ?uic.
  OPTIONAL { ?station wdt:P625 ?coord. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "hu,en". }
}'''
WIKIDATA_HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'osm_poi_matchmaker/1.0 (https://github.com/KAMI911/osm_poi_matchmaker; kami911@gmail.com)',
}
# Suffixes Wikidata's Hungarian station labels commonly carry that our GTFS stop_name
# doesn't (e.g. "Bocfölde vasútállomás" vs. "Bocfölde").
STATION_NAME_SUFFIXES = [' vasútállomás', ' vasúti megállóhely', ' megállóhely',
                         ' vasúti csomópont', ' rendező pályaudvar', ' pályaudvar']
# A lone same-named candidate only needs to survive ordinary coordinate slop between the
# two independent data sources; when several Wikidata items share a normalized name, a
# tighter radius is required to pick the right one with confidence.
MATCH_DISTANCE_SINGLE_M = 3000
MATCH_DISTANCE_MULTI_M = 1000


def _normalize_station_name(name):
    """Strip parenthetical disambiguators and a trailing Wikidata station-type suffix,
    so e.g. "Bocfölde vasútállomás" and "Bocfölde" compare equal."""
    if not name:
        return ''
    name = re.sub(r'\s*\([^)]*\)\s*', ' ', name).strip()
    lowered = name.lower()
    for suffix in STATION_NAME_SUFFIXES:
        if lowered.endswith(suffix):
            name = name[:-len(suffix)]
            break
    return ' '.join(name.split()).lower()


def _haversine_m(lat1, lon1, lat2, lon2):
    """Great-circle distance between two WGS84 points, in meters."""
    r = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _parse_wikidata_stations(raw_json):
    """Parse the Wikidata SPARQL JSON response into {normalized_name: [candidate, ...]},
    where each candidate is {'qid', 'uic', 'lat', 'lon'}."""
    stations_by_name = {}
    data = json.loads(raw_json)
    for binding in data.get('results', {}).get('bindings', []):
        label = binding.get('stationLabel', {}).get('value')
        uic = binding.get('uic', {}).get('value')
        coord = binding.get('coord', {}).get('value')
        station_uri = binding.get('station', {}).get('value')
        if not label or not uic or not coord or not station_uri:
            continue
        point = re.match(r'Point\(([-\d.]+) ([-\d.]+)\)', coord)
        if not point:
            continue
        lon, lat = float(point.group(1)), float(point.group(2))
        qid = station_uri.rsplit('/', 1)[-1]
        stations_by_name.setdefault(_normalize_station_name(label), []).append(
            {'qid': qid, 'uic': uic, 'lat': lat, 'lon': lon})
    return stations_by_name


def _match_wikidata_station(stations_by_name, stop_name, stop_lat, stop_lon):
    """Find the Wikidata station matching a GTFS stop by exact normalized name, then
    nearest coordinate among same-named candidates. Returns (uic, qid), or (None, None)
    if there's no name match or the nearest candidate is further away than the
    name-ambiguity-dependent distance threshold allows."""
    candidates = stations_by_name.get(_normalize_station_name(stop_name))
    if not candidates:
        return None, None
    best = min(candidates, key=lambda c: _haversine_m(stop_lat, stop_lon, c['lat'], c['lon']))
    distance = _haversine_m(stop_lat, stop_lon, best['lat'], best['lon'])
    threshold = MATCH_DISTANCE_SINGLE_M if len(candidates) == 1 else MATCH_DISTANCE_MULTI_M
    if distance > threshold:
        return None, None
    return best['uic'], best['qid']


class hu_mav(DataProvider):
    """Imports MÁV-START railway station locations in Hungary from MÁV's GTFS feed."""

    def contains(self):
        self.link = 'https://www.mavcsoport.hu/gtfs/gtfsMavMenetrend.zip'
        self.tags = {'railway': 'station', 'public_transport': 'station', 'operator': 'MÁV-START Zrt.',
                     'operator:addr': '1087 Budapest, Könyves Kálmán körút 54-60.', 'ref:HU:vatin': '13834492-2-44',
                     'ref:vatin': 'HU13834492', 'brand': 'MÁV-START', 'brand:wikidata': 'Q1180332',
                     'brand:wikipedia': 'hu:MÁV-START_Zrt.', 'contact:email': 'eszrevetel@mav-start.hu',
                     'contact:phone': '+36 1 349 4949',
                     'contact:facebook': 'mavstarthungary',
                     'contact:youtube': 'https://www.youtube.com/channel/UCcc8H-ND98GVF5OM5jJphWw',
                     'contact:instagram': 'mavstart'}
        self.filetype = FileType.zip
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def __load_wikidata_stations(self):
        """Fetch (and cache) Hungarian railway stations' UIC codes and Wikidata items,
        keyed by normalized name, for _match_wikidata_station(). Returns {} - not
        raises - if the Wikidata query service can't be reached, so a transient outage
        just means no uic_ref/wikidata tags get added this run, not a failed import."""
        cache_file = os.path.join(self.download_cache, 'hu_mav_wikidata_uic.json')
        url = '{}?{}'.format(WIKIDATA_SPARQL_URL, urlencode(
            {'query': WIKIDATA_SPARQL_QUERY, 'format': 'json'}))
        raw = save_downloaded_soup(url, cache_file, FileType.json, headers=WIKIDATA_HEADERS)
        if not raw:
            logging.warning('Could not load Wikidata UIC station data; '
                            'uic_ref/wikidata tags will be skipped this run.')
            return {}
        return _parse_wikidata_stations(raw)

    def types(self):
        humavstart = self.tags.copy()
        self.__types = [
            {'poi_code': 'humavstart', 'poi_common_name': 'MÁV START', 'poi_type': 'railway_station',
             'poi_tags': humavstart, 'poi_url_base': 'https://www.mavcsoport.hu',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 10, 'preserve_original_name': True, 'additional_ref_name': 'ref:mav',
             'gtfs_feed_id': 'HU-MAV', 'do_not_export_addr_tags': True },
        ]
        return self.__types

    def process(self):
        try:
            file = os.path.join(self.download_cache, self.filename)

            # skip downloading gtfs file, no official source for data
            # save_downloaded_soup('{}'.format(self.link), file, self.filetype, None, self.verify_link)

            if not os.path.isfile(file):
                raise Exception("MAV GTFS data file not found! Please provide an actual version in the right place with the right name (hu_mav.zip)")

            import gtfs_kit as gk
            feed = (gk.read_feed(file, dist_units='m'))
            # feed.validate()

            stops_df = feed.stops
            logging.debug('processing {} stops'.format(len(stops_df)))

            wikidata_stations = self.__load_wikidata_stations()

            start = timer()

            # processing stops
            for stop in stops_df.itertuples():
                try:
                    if stop.Index > 0 and stop.Index % 100 == 0:
                        now = timer()
                        per_item = (now - start) / stop.Index
                        remaining = (len(stops_df) - stop.Index) * per_item
                        logging.debug('stops {}/{}  elapsed={}  remaining={} total={}'.format(
                            stop.Index,
                            len(stops_df),
                            timedelta(seconds=round(now - start)),
                            timedelta(seconds=round(remaining)),
                            timedelta(seconds=round(per_item * len(stops_df)))
                        ))

                    # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                    self.data.name = stop.stop_name.strip()
                    self.data.code = 'humavstart'
                    self.data.poi_additional_ref = clean_string(stop.stop_id)
                    # As of the current MAV GTFS feed every stop has location_type=0 and no
                    # parent_station (no station/platform hierarchy), so this is currently a
                    # no-op - kept for when/if that changes, mirroring hu_volanbusz.py.
                    if pd.notna(stop.parent_station):
                        self.data.gtfs_parent_station = clean_string(stop.parent_station)
                        self.data.gtfs_location_type = clean_string(stop.location_type) \
                            if pd.notna(stop.location_type) else '0'
                    uic, qid = _match_wikidata_station(
                        wikidata_stations, stop.stop_name, stop.stop_lat, stop.stop_lon)
                    if uic:
                        self.data.uic_ref = uic
                        self.data.wikidata = qid
                    self.data.lat, self.data.lon = check_hu_boundary(stop.stop_lat, stop.stop_lon)
                    self.data.original = clean_string('id={} lat={} lon={} name={}'.format(
                        stop.stop_id,
                        stop.stop_lat,
                        stop.stop_lon,
                        stop.stop_name
                    ))
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(stop)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
