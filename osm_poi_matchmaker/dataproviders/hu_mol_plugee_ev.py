# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import time
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# Bounding box comfortably covering all of Hungary plus a small margin - the API
# returns raw markers by geographic bounds, not by country, and also serves several
# neighbouring countries on the same platform (see the adric=='HUN' filter below).
HU_BOUNDS = {'swLat': 45.6, 'swLng': 15.9, 'neLat': 48.7, 'neLng': 23.1}

# findSiteListDataBySiteIds silently returns success=False for large id batches
# (looks like rate limiting rather than a hard size limit - retrying a smaller/later
# batch succeeds), so fetch site details in small chunks with a short pause and a
# few retries between them instead of requesting everything in one call.
SITE_DETAIL_CHUNK_SIZE = 20
SITE_DETAIL_RETRIES = 3
SITE_DETAIL_RETRY_SLEEP = 2
SITE_DETAIL_CHUNK_SLEEP = 1

# GLS-style: the JSON's compact socket-type codes -> which POI schema field
# (quantity only; the API doesn't expose a per-type power rating, only price/kWh).
SOCKET_TYPE_FIELD = {
    'TYPE_2_MENNEKES': 'socket_type2_cable',
    'TYPE_2_TETHERED': 'socket_type2_cableless',
    'TYPE_4_CHADEMO': 'socket_chademo',
    'TYPE_COMBO_GERMANY': 'socket_type2_combo',
}


class hu_mol_plugee_ev(DataProvider):
    """Imports MOL Plugee EV charging station locations in Hungary from the same
    'evMap' widget API https://molplugee.hu/hu/evmap (embedded from
    account.molplugee.eu) uses: a two-step call, findSitesInBounds for the site ids
    within a bounding box, then findSiteListDataBySiteIds for full details. The
    platform is shared across several countries (CZ/SK/HR/SI/RO seen alongside HU
    in a Hungary-covering bounding box), so results are filtered to adric=='HUN'.
    """

    def contains(self):
        self.link = 'https://account.molplugee.eu/stationFacade/findSitesInBounds'
        self.tags = {'amenity': 'charging_station', 'authentication:app': 'yes', 'authentication:none': 'yes',
                     'brand': 'MOL', 'operator': 'MOL Nyrt.',
                     'operator:addr': '1117 Budapest, Október huszonharmadika utca 18.', 'fee': 'yes',
                     'parking:fee': 'no', 'opening_hours': '24/7', 'ref:vatin': 'HU10625790',
                     'ref:HU:vatin': '10625790-4-44', 'ref:HU:company': '01-10-041683',
                     'contact:email': 'info@molplugee.hu', 'contact:phone': '+36 1 998 9888',
                     'contact:website': 'https://molplugee.hu/', 'motorcar': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        humolplchs = self.tags.copy()
        self.__types = [
            {'poi_code': 'humolplchs', 'poi_common_name': 'MOL Plugee', 'poi_type': 'charging_station',
             'poi_tags': humolplchs, 'poi_url_base': 'https://molplugee.hu', 'poi_search_name': '(mol plugee|plugee)',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 300, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def _fetch_site_ids(self):
        soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                    self.filetype, False, post_data=HU_BOUNDS, json_body=True)
        if soup is None:
            return []
        text = json.loads(soup)
        if not text.get('success'):
            logging.warning('findSitesInBounds returned success=False: %s', text)
            return []
        return [s['siteId'] for s in text.get('data', [])]

    def _fetch_site_details(self, site_ids):
        detail_link = 'https://account.molplugee.eu/stationFacade/findSiteListDataBySiteIds'
        sites = []
        for i in range(0, len(site_ids), SITE_DETAIL_CHUNK_SIZE):
            chunk = site_ids[i:i + SITE_DETAIL_CHUNK_SIZE]
            chunk_file = os.path.join(self.download_cache, 'hu_mol_plugee_ev_sites_{:04d}.json'.format(i))
            text = None
            for attempt in range(SITE_DETAIL_RETRIES):
                soup = save_downloaded_soup(detail_link, chunk_file, self.filetype, False,
                                            post_data={'filterBySiteIds': chunk}, json_body=True)
                if soup is not None:
                    text = json.loads(soup)
                    if text.get('success'):
                        break
                logging.warning('findSiteListDataBySiteIds chunk %d failed (attempt %d/%d), retrying.',
                               i, attempt + 1, SITE_DETAIL_RETRIES)
                time.sleep(SITE_DETAIL_RETRY_SLEEP)
            if text is not None and text.get('success'):
                sites.extend(text.get('data', []))
            else:
                logging.error('Giving up on site-detail chunk starting at %d after %d attempts.',
                             i, SITE_DETAIL_RETRIES)
            time.sleep(SITE_DETAIL_CHUNK_SLEEP)
        return sites

    def process(self):
        try:
            site_ids = self._fetch_site_ids()
            if not site_ids:
                logging.warning('No site ids found in bounds. Skipping ...')
                return
            sites = self._fetch_site_details(site_ids)
            for poi_data in sites:
                try:
                    if poi_data.get('adric') != 'HUN':
                        continue
                    self.data.code = 'humolplchs'
                    self.data.ref = clean_string(poi_data.get('siteId'))
                    self.data.branch = clean_string(poi_data.get('dn'))
                    self.data.city = clean_city(poi_data.get('adrc'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(poi_data.get('adr1'))
                    self.data.original = clean_string(poi_data.get('adr1'))
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('la'), poi_data.get('lo'))
                    for socket in poi_data.get('smstdlst', []):
                        field = SOCKET_TYPE_FIELD.get(socket.get('st'))
                        if field is None:
                            logging.debug('Unmapped MOL Plugee socket type: %s', socket.get('st'))
                            continue
                        count = socket.get('sts')
                        if count is None:
                            continue
                        current = getattr(self.data, field)
                        setattr(self.data, field, count if current is None else current + count)
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
