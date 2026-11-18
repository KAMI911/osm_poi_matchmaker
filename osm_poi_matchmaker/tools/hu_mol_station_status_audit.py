# -*- coding: utf-8 -*-
"""Read-only report of MOL station-finder entries with a non-'1' stationStatus.

stationStatus == '1' means "currently MOL-operated" and is always imported by
hu_mol.py. Every other value is a mixed bag, NOT a clean "inactive" flag: it
covers sold-off/rebranded sites (name ends in " Orlen"), explicitly closed
ones ("... bezárt"), but also still-real entries such as CNG stations, the
Zöld Sziget waterway station, and internal/test sites - so hu_mol.py cannot
and does not skip on stationStatus alone. It only skips entries whose name
matches SKIP_NAME_PATTERN (trailing "Orlen"/"bezárt"): those are former MOL
sites whose OSM node is now correctly tagged with a competitor brand that the
AVOID_NAME regex excludes as a candidate match, so importing them as
'humolfu' could never match and would be reported as a false "new" POI.

This writes nothing to OSM, to any pipeline table, or to the live database -
it only reads the already-downloaded hu_mol.json cache (produced by a normal
hu_mol.py run) and prints/saves a CSV report for manual review of the full
non-'1' set, including the entries hu_mol.py does NOT skip (so it is worth
checking whether those still make sense to import as-is). Run standalone:

    python3 -m osm_poi_matchmaker.tools.hu_mol_station_status_audit
"""

import argparse
import csv
import json
import logging
import os
import sys

import re

try:
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('hu_mol_station_status_audit')

# Deliberately duplicated from hu_mol.SKIP_NAME_PATTERN, not imported from it: hu_mol.py
# pulls in the full DataProvider stack (DB driver included) just to define this regex,
# which this read-only, no-DB report has no other reason to depend on. Keep in sync by hand.
SKIP_NAME_PATTERN = re.compile(r'(orlen|bezárt)\s*$', re.IGNORECASE)

FIELDS = ['stationStatus', 'name', 'address', 'postcode', 'city', 'latitude', 'longitude',
          'skipped_by_hu_mol_py']


def collect_non_active(cache_path):
    with open(cache_path, encoding='utf-8') as f:
        stations = json.load(f)
    rows = []
    for station in stations or []:
        if station.get('stationStatus') == '1':
            continue
        gps = station.get('gpsPosition') or {}
        rows.append({
            'stationStatus': station.get('stationStatus'),
            'name': station.get('name'),
            'address': station.get('address'),
            'postcode': station.get('postcode'),
            'city': station.get('city_hu') or station.get('city'),
            'latitude': gps.get('latitude'),
            'longitude': gps.get('longitude'),
            'skipped_by_hu_mol_py': bool(SKIP_NAME_PATTERN.search(station.get('name') or '')),
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache-file', default=None,
                        help='Path to the cached hu_mol.json (default: hu_mol.json in the configured cache dir).')
    parser.add_argument('--out', default='hu_mol_station_status_audit.csv',
                        help='CSV output path (default: hu_mol_station_status_audit.csv in the current directory).')
    args = parser.parse_args()

    cache_path = args.cache_file or os.path.join(config.get_directory_cache_url(), 'hu_mol.json')
    log.info('Reading cached MOL station list: %s', cache_path)
    rows = collect_non_active(cache_path)
    skipped = sum(1 for r in rows if r['skipped_by_hu_mol_py'])
    log.info('%d entries with stationStatus != 1 found, %d of them skipped by hu_mol.py (name pattern match)',
            len(rows), skipped)

    with open(args.out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)
    log.info('Report written to %s', args.out)


if __name__ == '__main__':
    main()
