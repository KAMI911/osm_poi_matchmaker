# -*- coding: utf-8 -*-
"""
Hungarian local public transport importer (everywhere except Budapest/BKK, which -
being two orders of magnitude bigger and needing its own performance handling - has
its own dedicated hu_bkv.py).

Data source: gtfs.menetbrand.com ("A magyar GTFS"), a community-run aggregator that
republishes the official GTFS feed of one local transport operator per Hungarian
town/city it covers, all at the same URL shape:
  https://gtfs.menetbrand.com/download/{slug}/
(that URL serves the zip directly - Content-Disposition: attachment - there's no
separate HTML landing page to scrape first).

Like hu_bkv.py, each feed's stops.txt carries no per-row transport mode; the real
mode(s) serving a stop are only derivable via stop_times.txt -> trips.txt ->
routes.txt's route_type, and some feeds (Szeged, Veszprém, Tatabánya, Kaposvár, Paks,
Balaton shipping) use a real station/platform parent_station hierarchy while others
are flat. See hu_bkv.py's module docstring for the shared reasoning - the exact same
join/hierarchy handling is used here, just per-city instead of citywide.

poi_code: every town has its own local operator (Tüke Busz in Pécs, MVK in Miskolc,
V-Busz in Veszprém, ...), so - unlike modes within one city's own feed - these can't
share hu_bkv.py's poi_codes: poi_tags (which carries the operator name) is fixed
once at first registration for a given poi_code (get_or_create_common() never
updates an existing row), so reusing hu_bkv's 'hubkvbus' here would silently
attribute every other town's stops to BKK. Instead, poi_code is a deterministic hash
of (city slug, poi_type) - same rationale and format as hu_alltheplaces.py's - with
the real operator name recovered from poi_tags['operator'] and poi_common_name.
"""

try:
    import logging
    import sys
    import os
    import io
    import csv
    import hashlib
    import zipfile
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# (url slug, display name used if a feed's own agency.txt is unavailable/empty).
# "weekendbus" (a summer-season beach shuttle) and "bahart" (Balaton passenger
# shipping - all ferry routes) are included alongside the regular town bus networks;
# Budapest is deliberately excluded (see module docstring - hu_bkv.py covers it).
CITY_FEEDS = [
    ('pecs', 'Pécs'), ('miskolc', 'Miskolc'), ('szeged', 'Szeged'),
    ('szekesfehervar', 'Székesfehérvár'), ('szombathely', 'Szombathely'),
    ('veszprem', 'Veszprém'), ('tatabanya', 'Tatabánya'), ('kaposvar', 'Kaposvár'),
    ('dombovar', 'Dombóvár'), ('hodmezovasarhely', 'Hódmezővásárhely'),
    ('kiskunhalas', 'Kiskunhalas'), ('paks', 'Paks'), ('varpalota', 'Várpalota'),
    ('budaors', 'Budaörs'), ('bahart', 'Balatoni hajózás'), ('weekendbus', 'Weekendbus'),
]

# GTFS (extended) route_type -> internal poi_type - see hu_bkv.py for the base
# mapping. '800' is Szeged's own extended code for trolleybus (BKK's feed instead
# uses '11' for the same real-world mode) - GTFS extended route types aren't
# consistently numbered across independently-published feeds.
ROUTE_TYPE_TO_POI_TYPE = {
    '0': 'tram_stop',
    '1': 'railway_station',
    '2': 'railway_station',
    '3': 'bus_stop',
    '4': 'ferry_terminal',
    '11': 'bus_stop',
    '109': 'railway_station',
    '800': 'bus_stop',
}
_SKIP_LOCATION_TYPES = frozenset({'2', '3', '4'})
_CODE_PREFIX = 'loc'


def _city_type_poi_code(slug: str, poi_type: str) -> str:
    digest = hashlib.sha1('{}:{}'.format(slug, poi_type).encode('utf-8')).hexdigest()
    return '{}{}'.format(_CODE_PREFIX, digest[:7])


class hu_helyi_kozlekedes(DataProvider):
    """Imports local public transport stop locations for every Hungarian town/city
    covered by gtfs.menetbrand.com except Budapest (see hu_bkv.py). One poi_code is
    registered per (town, resolved mode) pair actually found in that town's feed -
    see module docstring for why poi_code can't be shared with hu_bkv.py or across
    towns."""

    def contains(self):
        self.tags = {'source': 'gtfs.menetbrand.com'}
        self.filetype = FileType.zip
        self.filename = 'hu_helyi_kozlekedes.zip'
        # {poi_code: (poi_type, slug, city_name, operator, [stop_row, ...])} -
        # populated by types(), consumed by process().
        self._city_data = {}

    def types(self) -> list:
        type_defs = {}
        for slug, fallback_name in CITY_FEEDS:
            try:
                rows = self._process_city(slug, fallback_name)
            except Exception as e:
                logging.warning('[hu_helyi_kozlekedes] Failed to process %s: %s', slug, e)
                logging.exception('Exception occurred')
                continue
            for poi_code, poi_type, city_name, operator, stop_rows in rows:
                # poi_code is a hash of (slug, poi_type), so each one can only ever
                # come from this one (city, mode) pair - no merging needed.
                self._city_data[poi_code] = (poi_type, slug, city_name, operator, stop_rows)
                if poi_code not in type_defs:
                    poi_tags = {'source': 'gtfs.menetbrand.com', 'source:city': city_name}
                    if operator:
                        poi_tags['operator'] = operator
                    if poi_type == 'bus_stop':
                        poi_tags.update({'highway': 'bus_stop', 'public_transport': 'platform', 'bus': 'yes'})
                    elif poi_type == 'tram_stop':
                        poi_tags.update({'railway': 'tram_stop', 'public_transport': 'platform', 'tram': 'yes'})
                    elif poi_type == 'railway_station':
                        poi_tags.update({'railway': 'station', 'public_transport': 'station'})
                    elif poi_type == 'ferry_terminal':
                        poi_tags.update({'amenity': 'ferry_terminal'})
                    type_defs[poi_code] = {
                        'poi_code': poi_code,
                        'poi_common_name': '{} ({})'.format(city_name, poi_type)[:64],
                        'poi_type': poi_type,
                        'poi_tags': poi_tags,
                        'poi_url_base': 'https://gtfs.menetbrand.com',
                        'poi_search_name': (operator or city_name).lower()[:128],
                        'preserve_original_name': True,
                        'do_not_export_addr_tags': True,
                        'additional_ref_name': 'gtfs:stop_id:HU-{}'.format(slug.upper())[:64],
                        'osm_search_distance_perfect': 400 if poi_type != 'tram_stop' else 300,
                        'osm_search_distance_safe': 100,
                        'osm_search_distance_unsafe': 15,
                    }
        logging.info('[hu_helyi_kozlekedes] Registered %d poi types across %d towns.',
                    len(type_defs), len(CITY_FEEDS))
        return list(type_defs.values())

    @staticmethod
    def _read_csv(zf, name):
        with zf.open(name) as raw:
            return list(csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8-sig')))

    def _process_city(self, slug, fallback_name):
        """Download and fully resolve one town's feed. Returns a list of
        (poi_code, poi_type, city_name, operator, [stop_row, ...]) tuples - one
        entry per distinct mode actually found in that town."""
        link = 'https://gtfs.menetbrand.com/download/{}/'.format(slug)
        cache_file = os.path.join(self.download_cache, 'hu_helyi_kozlekedes_{}.zip'.format(slug))
        save_downloaded_soup(link, cache_file, FileType.zip, False)
        if not os.path.isfile(cache_file):
            return []

        with zipfile.ZipFile(cache_file) as zf:
            names = set(zf.namelist())
            if not {'stops.txt', 'stop_times.txt', 'trips.txt', 'routes.txt'} <= names:
                logging.warning('[hu_helyi_kozlekedes] %s feed is missing required GTFS files.', slug)
                return []

            city_name = fallback_name
            operator = None
            if 'agency.txt' in names:
                agencies = self._read_csv(zf, 'agency.txt')
                if agencies:
                    operator = clean_string(agencies[0].get('agency_name'))

            trip_route = {row['trip_id']: row['route_id'] for row in self._read_csv(zf, 'trips.txt')}
            route_type = {row['route_id']: row['route_type'] for row in self._read_csv(zf, 'routes.txt')}

            stop_modes = {}
            with zf.open('stop_times.txt') as raw:
                for row in csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8-sig')):
                    rtype = route_type.get(trip_route.get(row['trip_id']))
                    if rtype is None:
                        continue
                    stop_modes.setdefault(row['stop_id'], set()).add(rtype)

            stops = self._read_csv(zf, 'stops.txt')

        children_by_parent = {}
        for stop in stops:
            parent = stop.get('parent_station')
            if parent:
                children_by_parent.setdefault(parent, []).append(stop['stop_id'])

        rows_by_type = {}
        for stop in stops:
            location_type = stop.get('location_type') or '0'
            if location_type in _SKIP_LOCATION_TYPES:
                continue
            parent = stop.get('parent_station')
            if location_type == '0' and parent:
                continue

            if location_type == '1':
                modes = set()
                for child_id in children_by_parent.get(stop['stop_id'], []):
                    modes |= stop_modes.get(child_id, set())
            else:
                modes = stop_modes.get(stop['stop_id'], set())

            poi_types = {ROUTE_TYPE_TO_POI_TYPE[m] for m in modes if m in ROUTE_TYPE_TO_POI_TYPE}
            for poi_type in poi_types:
                rows_by_type.setdefault(poi_type, []).append(stop)

        return [(_city_type_poi_code(slug, poi_type), poi_type, city_name, operator, rows)
                for poi_type, rows in rows_by_type.items()]

    def process(self):
        for poi_code, (poi_type, slug, city_name, operator, stop_rows) in self._city_data.items():
            for stop in stop_rows:
                try:
                    self.data.code = poi_code
                    self.data.name = clean_string(stop.get('stop_name'))
                    self.data.poi_additional_ref = clean_string(stop.get('stop_id'))
                    parent = stop.get('parent_station')
                    if parent:
                        self.data.gtfs_parent_station = clean_string(parent)
                        self.data.gtfs_location_type = stop.get('location_type') or '0'
                    self.data.lat, self.data.lon = check_hu_boundary(stop.get('stop_lat'), stop.get('stop_lon'))
                    self.data.original = clean_string('id={} lat={} lon={} name={}'.format(
                        stop.get('stop_id'), stop.get('stop_lat'), stop.get('stop_lon'), stop.get('stop_name')))
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(stop)
