# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import io
    import csv
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

# GTFS (extended) route_type -> internal poi_type. 0=tram, 1=metro/subway, 3=bus,
# 4=ferry, 11=trolleybus, 109=suburban railway (HÉV) - the last two are GTFS's
# "extended route types", which BKK's feed uses. Metro and HÉV both map to
# 'railway_station' since utils/poitypes.py's query for that type already covers
# railway='station' OR railway='halt'; bus and trolleybus share 'bus_stop' since
# both are tagged highway=bus_stop in OSM.
ROUTE_TYPE_TO_POI_TYPE = {
    '0': 'tram_stop',
    '1': 'railway_station',
    '3': 'bus_stop',
    '4': 'ferry_terminal',
    '11': 'bus_stop',
    '109': 'railway_station',
}
POI_TYPE_TO_CODE = {
    'bus_stop': 'hubkvbus',
    'tram_stop': 'hubkvtram',
    'railway_station': 'hubkvrail',
    'ferry_terminal': 'hubkvferry',
}
# location_type: 0/blank=stop or platform, 1=station, 2=entrance/exit, 3=generic
# node, 4=boarding area. Only 0 and 1 are real boarding points; 2-4 are structural
# (pathways, lifts, entrances) and never real transit POIs.
_SKIP_LOCATION_TYPES = frozenset({'2', '3', '4'})


class hu_bkv(DataProvider):
    """Imports BKK (Budapesti Közlekedési Központ) public transport stop locations
    from BKK's own public GTFS feed (go.bkk.hu/api/static/v1/public-gtfs/
    budapest_gtfs.zip) - freely downloadable, no authentication required (unlike
    hu_mav.py's GTFS feed). Bus, tram, metro, trolleybus, HÉV suburban rail and ferry
    stops are all mixed together in one stops.txt with no per-row mode field; the
    real mode(s) serving each stop are only derivable by joining
    stop_times.txt -> trips.txt -> routes.txt's route_type. This bypasses gtfs_kit
    (used by hu_mav.py/hu_volanbusz.py), which would load the citywide feed's
    ~340MB stop_times.txt into a full DataFrame, in favour of a lightweight
    streaming join with the stdlib csv module (~5-10s for the whole feed, measured
    against the 2026-09-05 export: 4.4M stop_times rows, 218k trips, 378 routes).

    Station hierarchy: a location_type=1 station's own row is imported once, with
    its mode(s) taken from the union of every child stop naming it as
    parent_station; those children are then skipped individually to avoid one POI
    per platform. A location_type=0/blank stop with no parent_station (ordinary
    bus/tram stops, and - per BKK's data - some legacy-style metro platforms never
    linked to a parent) is imported directly using its own mode(s). A stop actually
    served by more than one mode (e.g. a bus+tram interchange) is imported once per
    mode, since real OSM tagging for that case is normally separate nearby nodes
    per mode rather than one node carrying every tag at once."""

    def contains(self):
        self.link = 'https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip'
        self.tags = {'operator': 'Budapesti Közlekedési Központ Zrt.', 'network': 'BKK',
                     'network:wikidata': 'Q1417227', 'operator:wikidata': 'Q1417227',
                     'contact:website': 'https://bkk.hu'}
        self.filetype = FileType.zip
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        common = {'preserve_original_name': True, 'do_not_export_addr_tags': True,
                  # gtfs:stop_id:HU-BKK doesn't exist as a column in the current osm2pgsql
                  # import (BKK stops are essentially never tagged with it) - tried first
                  # anyway in case that changes, then ref:bkk, which IS a real, populated
                  # tag in OSM and - unlike the shared stop/platform name - uniquely
                  # identifies each BKK stop_id. See poi_base.py's ref_name_candidates.
                  'additional_ref_name': 'gtfs:stop_id:HU-BKK,ref:bkk', 'gtfs_feed_id': 'HU-BKK'}
        hubkvbus = {'highway': 'bus_stop', 'public_transport': 'platform', 'bus': 'yes'}
        hubkvbus.update(self.tags)
        hubkvtram = {'railway': 'tram_stop', 'public_transport': 'platform', 'tram': 'yes'}
        hubkvtram.update(self.tags)
        hubkvrail = {'railway': 'station', 'public_transport': 'station'}
        hubkvrail.update(self.tags)
        hubkvferry = {'amenity': 'ferry_terminal'}
        hubkvferry.update(self.tags)
        self.__types = [
            {'poi_code': 'hubkvbus', 'poi_common_name': 'BKK autóbuszmegálló', 'poi_type': 'bus_stop',
             'poi_tags': hubkvbus, 'poi_url_base': 'https://bkk.hu', 'poi_search_name': '(bkk|bkv)',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 10, **common},
            {'poi_code': 'hubkvtram', 'poi_common_name': 'BKK villamosmegálló', 'poi_type': 'tram_stop',
             'poi_tags': hubkvtram, 'poi_url_base': 'https://bkk.hu', 'poi_search_name': '(bkk|bkv)',
             'osm_search_distance_perfect': 300, 'osm_search_distance_safe': 80,
             'osm_search_distance_unsafe': 10, **common},
            {'poi_code': 'hubkvrail', 'poi_common_name': 'BKK állomás', 'poi_type': 'railway_station',
             'poi_tags': hubkvrail, 'poi_url_base': 'https://bkk.hu', 'poi_search_name': '(bkk|bkv)',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 150,
             'osm_search_distance_unsafe': 20, **common},
            {'poi_code': 'hubkvferry', 'poi_common_name': 'BKK kikötő', 'poi_type': 'ferry_terminal',
             'poi_tags': hubkvferry, 'poi_url_base': 'https://bkk.hu', 'poi_search_name': '(bkk|bkv)',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 20, **common},
        ]
        return self.__types

    @staticmethod
    def __build_stop_modes(zf):
        """Return {stop_id: {route_type, ...}} by streaming stop_times.txt (the
        biggest file by far, ~340MB) just once, joining through an in-memory
        trip_id->route_id and route_id->route_type mapping built from the much
        smaller trips.txt/routes.txt."""
        trip_route = {}
        with zf.open('trips.txt') as raw:
            for row in csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8')):
                trip_route[row['trip_id']] = row['route_id']

        route_type = {}
        with zf.open('routes.txt') as raw:
            for row in csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8')):
                route_type[row['route_id']] = row['route_type']

        stop_modes = {}
        with zf.open('stop_times.txt') as raw:
            for row in csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8')):
                rtype = route_type.get(trip_route.get(row['trip_id']))
                if rtype is None:
                    continue
                stop_modes.setdefault(row['stop_id'], set()).add(rtype)
        return stop_modes

    def process(self):
        try:
            file = os.path.join(self.download_cache, self.filename)
            save_downloaded_soup(self.link, file, self.filetype, False)
            if not os.path.isfile(file):
                logging.warning('BKK GTFS zip not found at %s. Skipping ...', file)
                return

            with zipfile.ZipFile(file) as zf:
                stop_modes = self.__build_stop_modes(zf)
                with zf.open('stops.txt') as raw:
                    stops = list(csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8')))

            children_by_parent = {}
            for stop in stops:
                parent = stop.get('parent_station')
                if parent:
                    children_by_parent.setdefault(parent, []).append(stop['stop_id'])

            for stop in stops:
                try:
                    location_type = stop.get('location_type') or '0'
                    if location_type in _SKIP_LOCATION_TYPES:
                        continue
                    parent = stop.get('parent_station')
                    if location_type == '0' and parent:
                        continue  # covered by the parent station's own row, below

                    if location_type == '1':
                        modes = set()
                        for child_id in children_by_parent.get(stop['stop_id'], []):
                            modes |= stop_modes.get(child_id, set())
                    else:
                        modes = stop_modes.get(stop['stop_id'], set())

                    poi_types = {ROUTE_TYPE_TO_POI_TYPE[m] for m in modes if m in ROUTE_TYPE_TO_POI_TYPE}
                    if not poi_types:
                        continue

                    for poi_type in poi_types:
                        self.data.code = POI_TYPE_TO_CODE[poi_type]
                        self.data.name = clean_string(stop.get('stop_name'))
                        self.data.poi_additional_ref = clean_string(stop.get('stop_id'))
                        if parent:
                            self.data.gtfs_parent_station = clean_string(parent)
                            self.data.gtfs_location_type = location_type
                        self.data.lat, self.data.lon = check_hu_boundary(
                            stop.get('stop_lat'), stop.get('stop_lon'))
                        self.data.original = clean_string('id={} lat={} lon={} name={}'.format(
                            stop.get('stop_id'), stop.get('stop_lat'), stop.get('stop_lon'),
                            stop.get('stop_name')))
                        self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(stop)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
