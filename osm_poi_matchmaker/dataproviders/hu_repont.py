# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_repont(DataProvider):
    """Imports REpont bottle-return vending machine locations in Hungary - MOHU's
    national DRS (deposit return system), scraped directly from map.mohu.hu's own
    search API (one POST call returns every point's id/name/address/coordinates;
    no per-point detail call needed - previously only covered indirectly via
    hu_alltheplaces.py's generic 'repont_hu' alltheplaces.xyz spider).

    Only subTypes containing 'automated' are imported as amenity=vending_machine.
    The API also returns 'manual' points (a shop counter that takes bottles back
    by hand, no machine on site) - tagging those as vending_machine would be
    factually wrong, so they're skipped here (unlike alltheplaces.xyz's own
    repont_hu spider, which tags both subtypes the same way)."""

    def contains(self):
        self.link = 'https://map.mohu.hu/api/Map/SearchPoisGeneralData'
        self.tags = {'amenity': 'vending_machine', 'vending': 'bottle_return',
                     'brand': 'REpont', 'brand:wikidata': 'Q130348902',
                     'operator': 'MOHU MOL Hulladékgazdálkodási Zrt.',
                     'operator:wikidata': 'Q130207606'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hurepont = self.tags.copy()
        self.__types = [
            {'poi_code': 'hurepont', 'poi_common_name': 'REpont', 'poi_type': 'vending_machine',
             'poi_tags': hurepont, 'poi_url_base': 'https://map.mohu.hu', 'poi_search_name': 'repont',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 50, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup(
                self.link, os.path.join(self.download_cache, self.filename), self.filetype, False,
                post_data={'wastePointTypes': ['repont'], 'hideDrsPoints': False}, json_body=True)
            if soup is None:
                return
            text = json.loads(soup)
            for poi_data in text:
                try:
                    if 'automated' not in (poi_data.get('subTypes') or []):
                        continue
                    self.data.code = 'hurepont'
                    self.data.ref = clean_string(poi_data.get('id'))
                    coords = poi_data.get('coordinates') or {}
                    self.data.lat, self.data.lon = check_hu_boundary(
                        coords.get('latitude'), coords.get('longitude'))
                    # The store/location the machine stands in (e.g. 'Lidl', 'Ecofamily') -
                    # not a brand of its own, just which outlet hosts this REpont machine.
                    self.data.branch = clean_string(poi_data.get('name'))
                    address = clean_string(poi_data.get('address'))
                    self.data.original = address
                    if address is not None:
                        self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                            self.data.conscriptionnumber = extract_all_address_waxeye(address)
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
