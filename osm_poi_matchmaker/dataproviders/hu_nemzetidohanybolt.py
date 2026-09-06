# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_nemzetidohanybolt(DataProvider):
    """Imports Nemzeti Dohánybolt (national tobacco shop) locations in Hungary from a JSONP feed.

    Note: as of 2026 the feed's domain (trafikok.nemzetidohany.hu) no longer resolves - the
    nemzetidohany.hu site now redirects outright to the successor regulator (sztfh.hu), whose
    public registry only publishes name/address/license number, not coordinates. This provider
    is left as-is (module loading and poi_code now fixed) for if/when a coordinate-bearing
    source becomes available again, but should not be enabled while self.link is dead.
    """

    def contains(self):
        self.link = 'http://trafikok.nemzetidohany.hu/mind.jsonp'
        self.tags = {'shop': 'tobacco'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hunemdoto = self.tags.copy()
        self.__types = [
            {'poi_code': 'hunemdoto', 'poi_common_name': 'Nemzeti dohánybolt', 'poi_type': 'tobacco',
             'poi_tags': hunemdoto, 'poi_url_base': 'https://www.nemzetidohany.hu/',
             'poi_search_name': '(nemzeti dohánybolt|dohánybolt)', 'osm_search_distance_safe': 200,
             'osm_search_distance_unsafe': 0},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype, False)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    try:
                        self.data.code = 'hunemdoto'
                        self.data.postcode = clean_string(poi_data.get('postcode'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.original = clean_string(poi_data.get('address'))
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('lat'), poi_data.get('lng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(poi_data.get('address'))
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
