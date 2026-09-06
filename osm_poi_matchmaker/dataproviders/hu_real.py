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
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_real(DataProvider):
    """Imports Reál convenience store locations in Hungary from the WordPress MapsMarkerPro
    plugin AJAX endpoint (https://real.hu/wp-admin/admin-ajax.php) real.hu's own store finder
    map uses. Reál is a COOP Hungary franchise brand of independently-run small shops, so most
    store names on the map are the individual shop's own name (e.g. "Bőség Sarok",
    "Németh Élelmiszer") rather than "Reál" itself - preserve_original_name is set accordingly."""

    def contains(self):
        self.link = 'https://real.hu/wp-admin/admin-ajax.php'
        self.post = {'action': 'mmp_map_markers', 'type': 'map'}
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'brand': 'Reál', 'brand:wikidata': 'Q100741414'}
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        hurealcon = {'shop': 'convenience'}
        hurealcon.update(self.tags)
        self.__types = [
            {'poi_code': 'hurealcon', 'poi_common_name': 'Reál', 'poi_type': 'shop', 'poi_tags': hurealcon,
             'poi_url_base': 'https://real.hu',
             'poi_search_name': '(reál pont|reálpont|reál élelmiszer|reál abc|reál)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 20, 'preserve_original_name': True},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                        self.filetype, False, self.post, headers=self.headers)
            if soup is None:
                return
            text = json.loads(soup)
            for feature in (text.get('data') or {}).get('features') or []:
                try:
                    props = feature.get('properties') or {}
                    geometry = feature.get('geometry') or {}
                    coords = geometry.get('coordinates') or []
                    if len(coords) < 2:
                        continue
                    self.data.code = 'hurealcon'
                    self.data.ref = clean_string(props.get('id'))
                    name = clean_string(props.get('name'))
                    if name is not None and 'reál' not in name.lower():
                        self.data.name = name
                        self.data.branch = None
                    else:
                        self.data.branch = name
                    address = clean_string(props.get('address'))
                    self.data.original = address
                    self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                        self.data.conscriptionnumber = extract_all_address_waxeye(address)
                    # GeoJSON coordinates are [longitude, latitude]
                    self.data.lat, self.data.lon = check_hu_boundary(coords[1], coords[0])
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(feature)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
