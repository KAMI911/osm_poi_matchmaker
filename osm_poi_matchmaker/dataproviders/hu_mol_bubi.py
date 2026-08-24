# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import traceback
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.libs.address import clean_string
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    
    sys.exit(128)


class hu_mol_bubi(DataProvider):
    
    def contains(self):
        self.link = ('https://core.urbansharing.com/public/api/v1/graphql'
                     '?operationName=dockGroupsExtended'
                     '&variables=%7B%22systemId%22%3A%22inurba-budapest%22%7D'
                     '&extensions=%7B%22clientLibrary%22%3A%7B%22name%22%3A%22%40apollo%2Fclient%22%2C'
                     '%22version%22%3A%224.1.3%22%7D%2C%22persistedQuery%22%3A%7B%22version%22%3A1%2C'
                     '%22sha256Hash%22%3A%221ef5051c2702f0fa61818c67d38125034b5ab99e483ccf36580461070705359d%22%7D%7D')
        self.tags = {'amenity': 'bicycle_rental', 'brand': 'MOL Bubi', 'brand:wikidata': 'Q16971969',
                     'brand:wikipedia': 'hu:MOL Bubi', 'operator': 'BKK', 'operator:wikidata': 'Q608917',
                     'operator:wikipedia': 'hu:Budapesti Közlekedési Központ', 'operator:type': 'public',
                     'operator:addr': '1075 Budapest Rumbach Sebestyén utca 19-21.', 'network': 'MOL Bubi',
                     'network:wikidata': 'Q16971969', 'network:wikipedia': 'hu:MOL Bubi',
                     'contact:phone': '+36 1 325 5255', 'contact:email': 'bkk@bkk.hu',
                     'contact:instagram': 'https://www.instagram.com/molbubi/',
                     'contact:facebook': 'https://www.facebook.com/molbubi',
                     'contact:youtube': 'https://www.youtube.com/user/bkkweb', 'contact:twitter': 'molbubi',
                     'fee': 'yes', 'payment:credit_cards': 'yes', 'payment:app': 'yes', 'charge': '50 HUF/minute'}
        
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)
    
    def types(self):
        hububibir = self.tags.copy()
        self.__types = [
            {'poi_code': 'hububibir', 'poi_common_name': 'MOL Bubi', 'poi_type': 'bicycle_rental',
             'poi_tags': hububibir, 'poi_url_base': 'https://molbubi.bkk.hu',
             'poi_search_name': '(mol bubi|mol-bubi|bubi)',
             'export_poi_name': False, 'do_not_export_addr_tags': True, 'osm_search_distance_perfect': 300,
             'osm_search_distance_unsafe': 1},
        ]
        return self.__types
    
    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                poi_datas = []
                try:
                    text = json.loads(soup)
                    poi_datas = text.get('data', {}).get('dockGroups') or []
                except Exception as e:
                    logging.exception('Exception occurred: %s', e)
                    logging.exception(traceback.format_exc())
                    logging.error(soup)

                for poi_data in poi_datas:
                    try:
                        if poi_data.get('state') != 'active' or not poi_data.get('enabled'):
                            continue
                        self.data.name = None
                        self.data.code = 'hububibir'
                        self.data.city = 'Budapest'
                        title = poi_data.get('title')
                        if title is not None and clean_string(title) is not None:
                            self.data.ref = clean_string(title)
                        # self.data.capacity = clean_string(poi_data.get('bike_racks'))
                        self.data.nonstop = True
                        coord = poi_data.get('coord') or {}
                        self.data.lat, self.data.lon = check_hu_boundary(coord.get('lat'), coord.get('lng'))
                        self.data.postcode = None
                        self.data.public_holiday_open = True
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
