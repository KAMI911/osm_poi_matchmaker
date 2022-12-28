# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.enums import WeekDaysLong
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_fire_station(DataProvider):

    def contains(self):
        self.link = 'https://www.katasztrofavedelem.hu/33856/tuzoltosagok-elhelyezkedese'
        self.tags = {'amenity': 'fire_station'}
        self.filetype = FileType.html
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    '''
    HTP = Hivatásos Tűzoltó-parancsnokság (1)
    KVŐ = Katasztrófavédelmi Őrs (2)
    ÖTE = Önkéntes Tűzoltó Egyesület (5)
    ÖTP = Önkormányzati Tűzoltó-parancsnokság (3)
    LTP = Létesítményi Tűzoltó-parancsnokság (?)

    fire_station:type=HTP | KVŐ | ÖTE | ÖTP | LTP`?
    
    Angol elnevezésekkel:
    HTP = main_station
    KVŐ = local_station
    ÖTE = voluntary
    ÖTP = municipal
    LTP = concern
    '''
    def types(self):
        hufiremsta = self.tags.copy()
        hufiremsta.update({'fire_station:type': 'main_station'})
        hufirelsta = self.tags.copy()
        hufirelsta.update({'fire_station:type': 'local_station'})
        hufirevsta = self.tags.copy()
        hufirevsta.update({'fire_station:type': 'voluntary'})
        hufireusta = self.tags.copy()
        hufireusta.update({'fire_station:type': 'municipal'})
        hufirecsta = self.tags.copy()
        hufirecsta.update({'fire_station:type': 'concern'})
        self.__types = [
            {'poi_code': 'hufiremsta', 'poi_common_name': 'Hivatásos Tűzoltó-parancsnokság',
             'poi_type': 'fire_station',
             'poi_tags': hufiremsta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(tűzoltó-parancsnokság)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 500,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufirelsta', 'poi_common_name': 'Katasztrófavédelmi Őrs',
             'poi_type': 'fire_station',
             'poi_tags': hufirelsta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(katasztrófavédelmi|örs)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 500,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufirevsta', 'poi_common_name': 'Önkéntes Tűzoltó Egyesület',
             'poi_type': 'fire_station',
             'poi_tags': hufirevsta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(tűzoltó|egyesület)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 500,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufireusta', 'poi_common_name': 'Önkormányzati Tűzoltó-parancsnokság',
             'poi_type': 'fire_station',
             'poi_tags': hufireusta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(tűzoltó|önkormányzati)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 500,
             'osm_search_distance_unsafe': 100},
            {'poi_code': 'hufirecsta', 'poi_common_name': 'Létesítményi Tűzoltó-parancsnokság',
             'poi_type': 'fire_station',
             'poi_tags': hufirecsta, 'poi_url_base': 'https://www.katasztrofavedelem.hu',
             'poi_search_name': '(tűzoltó|létesítményi)',
             'preserve_original_name': True, 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 500,
             'osm_search_distance_unsafe': 100},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype, None, self.verify_link)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                try:
                    pois = json.loads(soup.find(re.search('(\[.*\]);', soup.findAll('script')[-17].text)[1]))
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.print_exc())
                    logging.exception(pois)
                if pois is None:
                    return None
                for poi_data in pois:
                    try:
                        # Önkéntes Tűzoltó Egyesület
                        if poi_data.get('category') == 5:
                            self.data.code = 'hufirevsta'
                        elif poi_data.get('category') == 4:
                            logging.warning('Maybe this is not existing')
                        # Önkormányzati Tűzoltó-parancsnokság
                        elif poi_data.get('category') == 3:
                            self.data.code = 'hufireusta'
                            try:
                                self.data.name == poi_data.get('name').replace('ÖTP', 'Önkormányzati Tűzoltó-parancsnokság')
                            except Exception as err:
                                continue
                        # Katasztrófavédelmi Őrs
                        elif poi_data.get('category') == 2:
                            self.data.code = 'hufirelsta'
                            try:
                                    self.data.name == poi_data.get('name').replace('KŐ', 'Katasztrófavédelmi Őrs')
                            except Exception as err:
                                continue
                        # Hivatásos Tűzoltó-parancsnokság
                        elif poi_data.get('category') == 1:
                            self.data.code = 'hufiremsta'
                            try:
                                self.data.name == poi_data.get('name').replace('HTP', 'Hivatásos Tűzoltó-parancsnokság')
                            except Exception as err:
                                continue
                        else:
                            logging.warning('Unknown fire station category.')
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('latitude'),
                                                                         poi_data.get('longitude'))
                        self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
                            self.data.conscriptionnumber = extract_all_address(poi_data.get('address'))
                        self.data.phone(poi_data.get('phone'))
                        self.data.email(poi_data.get('email'))
                        self.data.original = clean_string(poi_data.get('address'))
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
