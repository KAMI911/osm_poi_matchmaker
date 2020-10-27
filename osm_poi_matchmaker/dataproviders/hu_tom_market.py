# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_osm_city_name_gpd
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_OTP, PAY_CASH
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_tom_market(DataProvider):

    def constains(self):
        self.link = 'https://tommarket.hu/boltkereso/get_stores/46?county=&settlement='
        self.tags = {'shop': 'convenience', 'name': 'Tom Market',
                     'contact:facebook': 'https://www.facebook.com/TOM.Market.Magyarorszag'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hutommacon = self.tags.copy()
        hutommacon.update(POS_OTP)
        hutommacon.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'hutommacon', 'poi_name': 'Tom Market', 'poi_type': 'shop',
             'poi_tags': hutommacon, 'poi_url_base': 'https://tommarket.hu', 'poi_search_name': 'tom market|tommarket',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                # script = soup.find('div', attrs={'data-stores':True})
                text = json.loads(str(soup))
                for poi_data in text['stores']:
                    try:
                        # Assign: code, postcode, city, name, branch, website, original, street, housenumber,
                        # conscriptionnumber, ref, geom
                        self.data.code = 'hutommacon'
                        if poi_data.get('name')[2] is not None and poi_data.get('name')[2] != '':
                            self.data.ref = poi_data.get('name')[2]
                        if poi_data.get('website') is not None and poi_data.get('website') != '':
                            self.data.website = poi_data.get('website')
                        else:
                            self.data.website = 'https://tommarket.hu'
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('lat'), poi_data.get('long'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(
                                poi_data.get('address'))
                        if poi_data.get('zip') is not None and poi_data.get('zip') != '':
                            self.data.postcode = poi_data.get('zip')
                        self.data.original = poi_data.get('address')
                        if poi_data.get('settlement') is not None and poi_data.get('settlement') != '':
                            self.data.city = clean_city(
                                poi_data.get('settlement'))
                        else:
                            self.data.city = query_osm_city_name_gpd(
                                self.session, self.data.lat, self.data.lon)
                        if poi_data.get('phone') is not None and poi_data.get('phone') != '':
                            self.data.phone = clean_phone_to_str(
                                poi_data.get('phone'))
                        if poi_data.get('email') is not None and poi_data.get('email') != '':
                            self.data.phone = poi_data.get('email').strip()
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.error(e)
                        logging.error(poi_data)
                        logging.exception('Exception occurred')

        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')
