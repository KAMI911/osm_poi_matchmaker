# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from timeit import default_timer as timer
    from datetime import timedelta
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup, extract_zip
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_opening_hours, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.enums import WeekDaysLong
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_volanbusz(DataProvider):

    def contains(self):
        self.link = 'https://gtfs.kti.hu/public-gtfs/volanbusz_gtfs.zip'
        self.tags = {'highway': 'bus_stop', 'public_transport': 'stop_position',
                     'bus': 'yes', 'operator': 'MÁV Személyszállítási Zrt.',
                     'operator:wikidata': 'Q1180332',
                     'network': 'Volán',
                     }
        self.filetype = FileType.zip
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huvolantra = self.tags.copy()
        self.__types = [
            {'poi_code': 'huvolantra', 'poi_common_name': 'Volánbusz', 'poi_type': 'bus_stop',
             'poi_tags': huvolantra, 'poi_url_base': 'https://www.volanbusz.hu', 'poi_search_name': 'volanbusz',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 10, 'preserve_original_name': True, 'additional_ref_name': 'gtfs:stop_id',
             'do_not_export_addr_tags': True},
        ]
        return self.__types

    def process(self):
        try:
            file = os.path.join(self.download_cache, self.filename)
            save_downloaded_soup('{}'.format(self.link), file, self.filetype, False, None, self.verify_link)

            import gtfs_kit as gk
            feed = (gk.read_feed(file, dist_units='m'))
            # feed.validate()

            stops_df = feed.stops
            logging.debug('processing {} stops'.format(len(stops_df)))

            start = timer()

            # processing stops
            for index, stop in stops_df.iterrows():
                try:
                    if index > 0 and index % 100 == 0:
                        now = timer()
                        per_item = (now - start) / index
                        remaining = (len(stops_df) - index) * per_item
                        logging.debug('stops {}/{}  elapsed={}  remaining={} total={}'.format(
                            index,
                            len(stops_df),
                            timedelta(seconds=round(now - start)),
                            timedelta(seconds=round(remaining)),
                            timedelta(seconds=round(per_item * len(stops_df)))
                        ))

                    # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                    self.data.name = stop.get('stop_name').strip()
                    self.data.code = 'huvolantra'
                    self.data.poi_additional_ref = clean_string(stop.get('stop_id'))
                    self.data.lat, self.data.lon = check_hu_boundary(stop.get('stop_lat'), stop.get('stop_lon'))
                    self.data.original = clean_string('id={} lat={} lon={} name={}'.format(
                        stop.get('stop_id'),
                        stop.get('stop_lat'),
                        stop.get('stop_lon'),
                        stop.get('stop_name')
                    ))
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(stop)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
