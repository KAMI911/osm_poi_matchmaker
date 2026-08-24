# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_phone_to_str, clean_string, extract_street_housenumber_better_2
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_osm_city_name_gpd
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_OTP, PAY_CASH
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

company_types = [' e.v.', ' ev.', ' kft.', ' KFT', ' bt.', ' bt']

class hu_tom_market(DataProvider):

    def contains(self):
        self.link = 'https://tommarket.hu/wp-admin/admin-ajax.php?action=asl_load_stores&asl_lang=&lang=hu_HU&load_all=1&layout=1'
        self.tags = {'shop': 'convenience', 'name': 'Tom Market',
                     'contact:facebook': 'TOM.Market.Magyarorszag'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hutommacon = self.tags.copy()
        hutommacon.update(POS_OTP)
        hutommacon.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hutommacon', 'poi_common_name': 'Tom Market', 'poi_type': 'shop',
             'poi_tags': hutommacon, 'poi_url_base': 'https://tommarket.hu', 'poi_search_name': 'tom market|tommarket',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                # The store locator plugin returns a plain JSON array of stores (no wrapping object).
                text = json.loads(str(soup))
                for poi_data in text or []:
                    try:
                        # Assign: code, postcode, city, name, branch, website, original, street, housenumber,
                        # conscriptionnumber, ref, geom
                        self.data.code = 'hutommacon'
                        name = clean_string(poi_data.get('title'))
                        if name is not None and name != '':
                            # Search list of multiple string fragments in a string
                            if any(map(name.__contains__, company_types)):
                                continue
                            else:
                                self.data.branch = name
                        self.data.website = None
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'), poi_data.get('lng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber =\
                            extract_street_housenumber_better_2(poi_data.get('street'))
                        self.data.city = clean_city(poi_data.get('city'))
                        self.data.postcode = clean_string(poi_data.get('postal_code'))
                        self.data.original = poi_data.get('street')
                        if clean_phone_to_str(poi_data.get('phone')) is not None:
                            self.data.phone = clean_phone_to_str(poi_data.get('phone'))
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.format_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
