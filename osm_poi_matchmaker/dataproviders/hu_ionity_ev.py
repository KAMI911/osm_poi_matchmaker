# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# The connector counts are grouped by power tier rather than plug type - in practice
# every IONITY high-power connector in this data set is CCS Type 2 Combo, so the
# tiers below (highest first) are only used to label socket_type2_combo_output with
# the strongest power actually present at a given station.
COMBO_POWER_TIERS_KW = (600, 500, 400, 350, 200, 50)


class hu_ionity_ev(DataProvider):
    """Imports IONITY EV charging station locations in Hungary from the public
    station-map JSON (https://wf-assets.com/ionity/mapdata.json) used by the
    interactive map embedded on https://www.ionity.eu/network."""

    def contains(self):
        self.link = 'https://wf-assets.com/ionity/mapdata.json'
        self.tags = {'amenity': 'charging_station', 'brand': 'IONITY', 'network': 'IONITY',
                     'operator': 'IONITY GmbH', 'contact:website': 'https://ionity.eu',
                     'authentication:app': 'yes', 'authentication:membership_card': 'yes',
                     'fee': 'yes', 'parking:fee': 'no', 'opening_hours': '24/7', 'motorcar': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huionitychs = self.tags.copy()
        self.__types = [
            {'poi_code': 'huionitychs', 'poi_common_name': 'IONITY', 'poi_type': 'charging_station',
             'poi_tags': huionitychs, 'poi_url_base': 'https://ionity.eu', 'poi_search_name': '(ionity)',
             'poi_search_avoid_name': '(mobiliti|mobility|e-mobi|emobi|e-töltőpont|plugee|tesla|supercharger|tea|'
                                      'e.on|eon|e-on|shell|alte-go|parkl|elektro profi|elmű|omv|emotion|'
                                      'optimum way|lidl|ev24|freecharger|ecotap|penny|polyfazer|rolecev|rollin|'
                                      'toka|voltie|zse|audi|chargepoint|city gecko|greengo|innogy|joinon|enel|'
                                      'virta|stormi|hep elen|cyeb|ampernative|mvm)',
             'osm_search_distance_perfect': 400,
             'osm_search_distance_safe': 100, 'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def process(self):
        try:
            logging.info('Processing file: {}'.format(self.link))
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is None:
                return
            text = json.loads(soup)
            for poi_data in text.get('LocationDetails', []):
                try:
                    if (poi_data.get('country') or '').lower() != 'hungary':
                        continue
                    if poi_data.get('state') != 'active':
                        continue
                    self.data.code = 'huionitychs'
                    self.data.branch = poi_data.get('name')
                    self.data.lat, self.data.lon = check_hu_boundary(
                        poi_data.get('latitude'), poi_data.get('longitude'))

                    ac_count = poi_data.get('connectorsAC') or 0
                    combo_count = (poi_data.get('connectorsTotal') or 0) - ac_count
                    self.data.socket_type2_cable = ac_count if ac_count > 0 else None
                    if combo_count > 0:
                        self.data.socket_type2_combo = combo_count
                        for tier in COMBO_POWER_TIERS_KW:
                            if poi_data.get('connectors{}kw'.format(tier)):
                                self.data.socket_type2_combo_output = '{} kW'.format(tier)
                                break
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
