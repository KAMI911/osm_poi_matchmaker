# -*- coding: utf-8 -*-
import numpy as np

try:
    import logging
    import sys
    import os
    import math
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_url, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


PATTERN_STATION = re.compile(r'^(?P<place>.*?)[,\s]*(?P<num>\d{4,6})\s*-\s*Mobiliti$')

PLUG_FIELD = {('type2', True): 'socket_type2_cable', ('type2', False): 'socket_type2_cableless',
              ('chademo', None): 'socket_chademo', ('ccs', None): 'socket_type2_combo',
              ('schuko', None): 'socket_schuko'}
# Socket types that only have a count and an output column (their current/voltage is fixed by the standard)
COUNT_OUTPUT_ONLY = ('socket_schuko', 'socket_cee_red_16a', 'socket_cee_red_32a')


def format_kw(watt):
    """22000 -> '22 kW', 7400 -> '7.4 kW' (no truncation to whole kW)."""
    return '{:g} kW'.format(round(watt / 1000, 1))


def plug_field(port):
    """Map an OCPI EVSE to the POIDataset socket field it is counted in, or None if there is no field
    for it. Tesla's connectors are the standard European ones, and an IEC 60309 5-pin socket is the red CEE
    one, tagged by its amperage."""
    plug = (port.get('plugType') or '').lower()
    if plug == 'type2':
        return PLUG_FIELD[(plug, bool(port.get('cableAttached')))]
    if plug == 'tesla':
        # OSM: socket:tesla_supercharger is deprecated, in the EU use the standard connector instead. Tesla's
        # DC chargers here are CCS, the AC (destination/wall connector) ones have a Type 2 cable.
        return 'socket_type2_combo' if (port.get('currentType') or '').upper() == 'DC' else 'socket_type2_cable'
    if plug == 'iec60309_5':
        current = port.get('current') or 0
        return 'socket_cee_red_16a' if current <= 16 else 'socket_cee_red_32a' if current <= 32 else None
    return PLUG_FIELD.get((plug, None))


def join_desc(values, fmt=str):
    """Distinct values, highest first, as an OSM ';' list ('22 kW;11 kW')."""
    return ';'.join(fmt(v) for v in sorted(set(values), reverse=True))


def summarize_evses(evses):
    """Collapse the EVSE list of a (merged) site into POIDataset socket fields: the connector count per
    socket type is summed, output/current/voltage list every distinct value of that type, highest first,
    and capacity is the total number of connectors, plug types we have no tag for (IEC 60309 over 32 A)
    included."""
    fields = {}
    values = {}
    seen = set()
    capacity = 0
    for port in evses or []:
        evse_id = port.get('evseId')
        if evse_id is not None:
            if evse_id in seen:
                continue
            seen.add(evse_id)
        quantity = port.get('quantity') or 1
        capacity += quantity
        field = plug_field(port)
        if field is None:
            continue
        fields[field] = fields.get(field, 0) + quantity
        for key in ('power', 'current', 'voltage'):
            if port.get(key):
                values.setdefault((field, key), []).append(int(port[key]))
    for (field, key), found in values.items():
        if key == 'power':
            fields[field + '_output'] = join_desc(found, format_kw)
        elif field not in COUNT_OUTPUT_ONLY:
            fields['{}_{}'.format(field, key)] = join_desc(found)
    if capacity:
        fields['capacity'] = capacity
    return fields


# The feed carries the whole OCPI roaming network, not just Mobiliti's own stations. The other operators'
# sites are imported under their own name instead of as 'Mobiliti' (code, name, operator tag, feed operator
# pattern). Operators with their own dedicated import (MOL, OMV, IONITY) are skipped as duplicates.
FOREIGN_OPERATORS = [
    ('huteachs', 'TEA', 'TEA Mobilitás Kft.', r'^(tea mobilit|hu\*tea)'),
    ('hulidlchs', 'Lidl', 'Lidl Magyarország Bt.', r'^(lidl|hu\*ldl)'),
    ('hueonchs', 'E.ON', 'E.ON', r'^e\.?on\b'),
    ('huelmuchs', 'ELMŰ', 'ELMŰ', r'^elm[űu]\b'),
    ('huparklchs', 'Parkl', 'Parkl', r'^parkl'),
    ('hushellrchs', 'Shell Recharge', 'Shell Recharge', r'^shell'),
    ('huv4pluschs', 'V4plus', 'V4plus Kft.', r'^v4plus'),
    ('huoptimumchs', 'Optimum Way', 'Optimum Way', r'^optimum way'),
    ('huevsolchs', 'EVsOLUTION', 'EVsOLUTION Kft.', r'^evsolution'),
    ('hupennychs', 'Penny', 'Penny Market', r'^penny'),
    ('huevdirchs', 'evDirect', 'evDirect', r'^evdirect'),
    ('huevoprochs', 'evopro', 'evopro', r'^evopro'),
    ('huaudichs', 'Audi', 'Audi Hungária Zrt.', r'^hu\*aud'),
    ('hutslachs', 'Tesla Supercharger', 'Tesla', r'^tesla$'),
]
SKIPPED_OPERATORS = re.compile(r'^(mol\b|omv\b|hu\*ioy|ionity)', re.IGNORECASE)
MOBILITI_OPERATOR = re.compile(r'mobil[it]+i|^mvm|^nkm', re.IGNORECASE)
OTHER_OPERATOR_CODE = 'humobothchs'


def classify_operator(operator_name):
    """Return (poi_code, display name) for a feed record by its operator, or None for a record that
    another import covers. No operator name, or an MVM/NKM Mobiliti one, is Mobiliti's own station;
    a not listed operator goes to the generic code and keeps its own name."""
    name = (operator_name or '').strip()
    if not name or MOBILITI_OPERATOR.search(name):
        return 'humobilchs', None
    if SKIPPED_OPERATORS.search(name):
        return None
    for code, display, _, pattern in FOREIGN_OPERATORS:
        if re.search(pattern, name, re.IGNORECASE):
            return code, display
    return OTHER_OPERATOR_CODE, name


class hu_mobiliti_ev(DataProvider):
    """Imports Mobiliti EV charging station locations in Hungary from Mobiliti's OCPI locations API."""

    def contains(self):
        self.link = 'https://api.mobiliti.hu/ocpi-location/api/v1/own-and-foreign-locations'
        self.tags = {'amenity': 'charging_station', 'authentication:app': 'yes', 'authentication:none': 'yes',
                     'authentication:membership_card': 'yes', 'operator': 'NKM Mobilitás Kft.',
                     'operator:addr': '1081 Budapest, II. János Pál pápa tér 20.', 'fee': 'yes', 'parking:fee': 'no',
                     'opening_hours': '24/7', 'ref:vatin': 'HU23443486', 'ref:HU:vatin': '23443486-2-42',
                     'ref:HU:company': '01-09-965868', 'contact:website': 'https://www.mobiliti.hu/emobilitas',
                     'contact:email': 'help@mobiliti.hu', 'contact:phone': '+36 62 565 758', }
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [
            {'poi_code': 'humobilchs', 'poi_common_name': 'Mobiliti', 'poi_type': 'charging_station',
             'poi_tags': self.tags, 'poi_url_base': 'https://www.mobiliti.hu',
             'poi_search_name': '(mobiliti|mobility|e-mobi|emobi|e-töltőpont)',
             'poi_search_avoid_name': '(tesla|supercharger|plugee|tea|e.on|eon|e-on|shell|ionity|alte-go|parkl|'
                                      'elektro profi|elmű|omv|emotion|optimum way|lidl|ev24|freecharger|ecotap|'
                                      'penny|polyfazer|rolecev|rollin|toka|voltie|zse|audi|chargepoint|city gecko|'
                                      'greengo|innogy|joinon|enel|virta|stormi|hep elen|cyeb|ampernative|mvm)',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 400,
             'osm_search_distance_safe': 150, 'osm_search_distance_unsafe': 10},
        ]
        for code, display, operator, pattern in FOREIGN_OPERATORS:
            self.__types.append(
                {'poi_code': code, 'poi_common_name': display, 'poi_type': 'charging_station',
                 'poi_tags': {'amenity': 'charging_station', 'operator': operator},
                 'poi_url_base': '', 'poi_search_name': '(' + display.lower().replace('.', '.?') + ')',
                 'poi_search_avoid_name': '(mobiliti)',
                 'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 150,
                 'osm_search_distance_unsafe': 10})
        self.__types.append(
            {'poi_code': OTHER_OPERATOR_CODE, 'poi_common_name': 'EV töltő', 'poi_type': 'charging_station',
             'poi_tags': {'amenity': 'charging_station'}, 'poi_url_base': '', 'poi_search_name': None,
             'poi_search_avoid_name': '(mobiliti)',
             'osm_search_distance_perfect': 100, 'osm_search_distance_safe': 50, 'osm_search_distance_unsafe': 10})
        return self.__types

    @staticmethod
    def merge_nearby_sites(locations, max_distance=50):
        """The OCPI feed lists every charge point as its own location, so one site shows up as several
        records with different ids a few meters apart. OSM maps a site as a single node, so merge records of
        the same operator and postcode within max_distance meters (single linkage) into one, the one with the
        lowest id as the representative, collecting the evses of all of them."""
        def coords(loc):
            try:
                return float(loc.get('latitude')), float(loc.get('longitude'))
            except (TypeError, ValueError):
                return None

        def key(loc):
            return (loc.get('operator') or {}).get('name'), loc.get('postalCode')

        ordered = sorted((loc for loc in locations if coords(loc) is not None), key=lambda loc: str(loc.get('id')))
        merged = [loc for loc in locations if coords(loc) is None]
        sites = []  # [representative, [(lat, lon), ...]]
        for loc in ordered:
            lat, lon = coords(loc)
            cos_lat = math.cos(math.radians(lat))
            for site in sites:
                if key(site[0]) == key(loc) and any(
                        math.hypot((lon - lon2) * 111320 * cos_lat, (lat - lat2) * 110540) < max_distance
                        for lat2, lon2 in site[1]):
                    site[1].append((lat, lon))
                    site[0]['evses'] = (site[0].get('evses') or []) + (loc.get('evses') or [])
                    break
            else:
                site_loc = dict(loc)
                site_loc['evses'] = list(loc.get('evses') or [])
                sites.append([site_loc, [(lat, lon)]])
        merged.extend(site[0] for site in sites)
        return merged

    def process(self):
        try:
            logging.info('Processing file: {}'.format(self.link))
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = self.merge_nearby_sites(json.loads(soup))
                for poi_data in text:
                    try:
                        classified = classify_operator((poi_data.get('operator') or {}).get('name'))
                        if classified is None:
                            continue
                        self.data.code, operator_name = classified
                        if operator_name is not None:
                            self.data.name = operator_name
                        # Only Mobiliti's own stations have a ref that is meaningful in OSM
                        if self.data.code == 'humobilchs':
                            self.data.ref = clean_string(poi_data.get('id'))
                        # 'Oroszlány, Uszoda, 14006 - Mobiliti': in OSM the station number is the ref, the branch is
                        # only the place (the ' - Mobiliti' suffix repeats the name)
                        station = PATTERN_STATION.match(poi_data.get('name') or '')
                        if self.data.code == 'humobilchs' and station and station.group('place'):
                            self.data.ref = station.group('num')
                            self.data.branch = clean_string(station.group('place'))
                        else:
                            self.data.branch = clean_string(poi_data.get('name'))
                        self.data.postcode = clean_string(poi_data.get('postalCode'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(
                                poi_data.get('Cím'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(poi_data.get('address'))
                        self.data.original = poi_data.get('address')
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('latitude'), poi_data.get('longitude'))
                        for field, value in summarize_evses(poi_data.get('evses')).items():
                            setattr(self.data, field, value)
                        self.data.manufacturer = poi_data.get('manufacturer')
                        #self.data.model = poi_data.get('Típus')
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
