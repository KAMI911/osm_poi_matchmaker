# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_string, clean_url
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_shell(DataProvider):

    def contains(self):
        self.link = 'https://shellgsllocator.geoapp.me/api/v1/locations/within_bounds?sw[]=45.48&sw[]=16.05&ne[]=48.35&ne[]=22.58&autoload=true&travel_mode=driving&avoid_tolls=false&avoid_highways=false&avoid_ferries=false&corridor_radius=5&driving_distances=false&format=json'
        self.tags = {'amenity': 'fuel', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}
        self.tags.update(POS_HU_GEN)
        self.tags.update({'loyalty_card': 'yes'})
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hushellfu = self.tags.copy()
        hushellfu.update({'brand': 'Shell', 'contact:phone': '+36 1 480 1114',
                          'contact:fax': '+36 1 999 8673', 'contact:website': 'https://shell.hu/',
                          'contact:facebook': 'https://www.facebook.com/ShellMagyarorszag/', 'contact:twitter': 'shell',
                          'brand:wikidata': 'Q154950', 'brand:wikipedia': 'hu:Royal Dutch Shell',
                          'air_conditioning': 'yes'})
        self.__types = [
            {'poi_code': 'hushellfu', 'poi_common_name': 'Shell', 'poi_type': 'fuel', 'poi_tags': hushellfu,
             'poi_url_base': 'https://shell.hu', 'poi_search_name': 'shell',
             'poi_search_avoid_name': '(mol|m. petrol|avia|lukoil|hunoil)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 60},
        ]
        return self.__types

    def process(self):

        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    try:
                        if poi_data.get('country_code') == 'HU':
                            logging.debug('Shell fuel station in Hungary')
                        else:
                            logging.info('Shell fuel station NOT in Hungary')
                            continue
                        self.data.code = 'hushellfu'
                        self.data.website = clean_url(poi_data.get('website_url')) if ('website_url' in poi_data and poi_data.get('website_url') != '') else 'https://shell.hu/'
                        self.data.postcode = clean_string(poi_data.get('postcode')) if ('postcode' in poi_data and poi_data.get('postcode') != '') else None
                        street_tmp = poi_data.get('address').lower().split()
                        for i in range(0, len(street_tmp) - 2):
                            street_tmp[i] = street_tmp[i].capitalize()
                        street_tmp = ' '.join(street_tmp)
                        if 'city' in poi_data and poi_data.get('city') != '':
                            self.data.city = clean_city(poi_data.get('city').title())
                        else:
                            if 'name' in poi_data and poi_data.get('name') != '':
                                self.data.city = clean_city(
                                    poi_data.get('name').title())
                            else:
                                self.data.city = None
                        if 'name' in poi_data and poi_data.get('name') != '':
                            self.data.branch = poi_data.get('name').strip()
                        if 'twenty_four_hour' in poi_data.get('amenities'):
                            self.data.nonstop = True
                            self.data.public_holiday_open = True
                        self.data.original = poi_data.get('address') if ('address' in poi_data and poi_data.get('address') != '') else None
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'),
                                                                        poi_data.get('lng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(
                            street_tmp)
                        self.data.phone = clean_phone_to_str(str(poi_data.get('telephone'))) if ('telephone' in poi_data and poi_data.get('telephone') != '') else None
                        self.data.email = None
                        self.data.fuel_octane_95 = True
                        self.data.fuel_diesel = True
                        self.data.fuel_octane_100 = True
                        self.data.fuel_diesel_gtl = True
                        if 'air_and_water' in poi_data.get('amenities'):
                            self.data.compressed_air = True
                        # TODO: Separete adblue_pack, adblue_car and adblue_truck
                        if 'adblue_pack' in poi_data.get('amenities') or 'adblue_car' in poi_data.get('amenities') or 'adblue_truck' in poi_data.get('amenities'):
                            self.data.fuel_adblue = True
                        if 'hot_food' in poi_data.get('amenities'):
                            self.data.restaurant = True
                        if 'bakery_shop' in poi_data.get('amenities') or 'food_offerings' in poi_data.get('amenities'):
                            self.data.food = True
                        if 'hgv_lane' in poi_data.get('amenities'):
                            self.data.truck = True
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
