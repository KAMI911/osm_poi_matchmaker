# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import time
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

DETAILS_RETRIES = 2
DETAILS_RETRY_SLEEP = 2
DETAILS_REQUEST_SLEEP = 0.2

# The site-list endpoint (same one hu_omv.py uses) only returns fuel-station fields -
# whether a given station also hosts OMV E-MOTION charging only shows up in each
# site's free-text "siteFeatures" list from details.php, as an entry like
# "Elektromos autó töltő - OMV E-MOTION, Alpitronic, 200kW; 4CCS". Formatting of
# that string isn't fully consistent (manufacturer/power/connector count are
# sometimes missing or ordered differently), so kW and CCS-connector-count are
# extracted best-effort and left None when the text doesn't spell them out.
POWER_RE = re.compile(r'(\d+)\s*kw', re.IGNORECASE)
CCS_COUNT_RE = re.compile(r'(\d+)\s*x?\s*ccs', re.IGNORECASE)
# Charger hardware manufacturers seen in real siteFeatures text (confirmed live
# against every cached site detail) - add to this as new ones show up. A plain
# substring search rather than a comma-position parse, since the manufacturer's
# position/formatting in the free text varies ('Alpitronic, 200kW; 4CCS' vs
# '1 x HYC150 Alpitronic' vs 'ALPITRONIC HYC 200 kW 2 CCS').
_MANUFACTURER_CANONICAL = {'alpitronic': 'Alpitronic', 'ekoenergetyka': 'Ekoenergetyka', 'flexpole': 'Flexpole'}
MANUFACTURER_RE = re.compile('|'.join(_MANUFACTURER_CANONICAL), re.IGNORECASE)


class hu_omv_ev(DataProvider):
    """Imports OMV E-MOTION EV charging station locations in Hungary. OMV E-MOTION
    chargers are installed at a subset of OMV's own fuel stations rather than
    listed separately, so this provider reuses the same Wigeogis-hosted station
    finder as hu_omv.py to get the full station list, then calls the finder's
    per-site details.php for each one and keeps only stations whose siteFeatures
    mention OMV E-MOTION charging."""

    def contains(self):
        self.link = 'https://app.wigeogis.com/kunden/omv/data/getresults.php'
        self.details_link = 'https://app.wigeogis.com/kunden/omv/data/details.php'
        self.tags = {'amenity': 'charging_station', 'brand': 'OMV E-MOTION', 'network': 'OMV E-MOTION',
                     'operator': 'OMV Hungária Kft.',
                     'operator:addr': '1117 Budapest, Október huszonharmadika utca 6-10 5. emelet 5/A.',
                     'ref:HU:vatin': '10542925-2-44', 'ref:vatin': 'HU10542925',
                     'ref:HU:company': '01-09-071584', 'contact:email': 'info.hungary@omv.com',
                     'contact:website': 'https://www.omv.hu/hu/toltoallomas-kereso',
                     'authentication:app': 'yes', 'fee': 'yes', 'parking:fee': 'no', 'motorcar': 'yes'}
        self.post = {'BRAND': 'OMV', 'CTRISO': 'HUN', 'MODE': 'NEXTDOOR', 'VEHICLE': 'CAR', 'ANZ': '20', 'QRY': ''}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huomvchs = self.tags.copy()
        self.__types = [
            {'poi_code': 'huomvchs', 'poi_common_name': 'OMV E-MOTION', 'poi_type': 'charging_station',
             'poi_tags': huomvchs, 'poi_url_base': 'https://www.omv.hu', 'poi_search_name': '(omv e-motion|omv)',
             'poi_search_avoid_name': '(mobiliti|mobility|e-mobi|emobi|e-töltőpont|plugee|tesla|supercharger|tea|'
                                      'e.on|eon|e-on|shell|ionity|alte-go|parkl|elektro profi|elmű|lidl|ev24|'
                                      'freecharger|ecotap|penny|polyfazer|rolecev|rollin|toka|voltie|zse|audi|'
                                      'chargepoint|city gecko|greengo|innogy|joinon|enel|virta|stormi|hep elen|'
                                      'cyeb|ampernative|mvm)',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 300, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def _fetch_site_list(self):
        soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                    self.filetype, False, self.post)
        if soup is None:
            return []
        return json.loads(soup)

    def _fetch_site_details(self, sid):
        detail_post = {'BRAND': 'OMV', 'CTRISO': 'HUN', 'LNG': 'HU', 'MODE': 'NEXTDOOR', 'ID': sid}
        detail_file = os.path.join(self.download_cache, 'hu_omv_ev_site_{}.json'.format(sid))
        for attempt in range(DETAILS_RETRIES):
            soup = save_downloaded_soup(self.details_link, detail_file, self.filetype, False, detail_post)
            if soup is not None:
                return json.loads(soup)
            logging.warning('details.php failed for site %s (attempt %d/%d), retrying.',
                           sid, attempt + 1, DETAILS_RETRIES)
            time.sleep(DETAILS_RETRY_SLEEP)
        logging.error('Giving up on site details for %s after %d attempts.', sid, DETAILS_RETRIES)
        return None

    @staticmethod
    def _find_charging_feature(details):
        for feature in details.get('siteFeatures', []):
            title = feature.get('title') or ''
            if 'e-motion' in title.lower():
                return title
        return None

    def process(self):
        try:
            sites = self._fetch_site_list()
            if not sites:
                logging.warning('No OMV stations found. Skipping ...')
                return
            for site in sites:
                try:
                    sid = site.get('sid')
                    if sid is None:
                        continue
                    details = self._fetch_site_details(sid)
                    time.sleep(DETAILS_REQUEST_SLEEP)
                    if details is None:
                        continue
                    charging_feature = self._find_charging_feature(details)
                    if charging_feature is None:
                        continue

                    self.data.code = 'huomvchs'
                    self.data.ref = clean_string(sid)
                    self.data.postcode = clean_string(site.get('postcode'))
                    self.data.city = clean_city(site.get('town_l'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(site.get('address_l'))
                    self.data.original = clean_string(site.get('address_l'))
                    self.data.phone = clean_phone_to_str(site.get('telnr'))
                    self.data.lat, self.data.lon = check_hu_boundary(site.get('y'), site.get('x'))

                    power_match = POWER_RE.search(charging_feature)
                    ccs_match = CCS_COUNT_RE.search(charging_feature)
                    manufacturer_match = MANUFACTURER_RE.search(charging_feature)
                    if ccs_match is not None:
                        self.data.socket_type2_combo = int(ccs_match.group(1))
                    if power_match is not None:
                        self.data.socket_type2_combo_output = '{} kW'.format(power_match.group(1))
                    if manufacturer_match is not None:
                        self.data.manufacturer = _MANUFACTURER_CANONICAL[manufacturer_match.group(0).lower()]
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(site)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
