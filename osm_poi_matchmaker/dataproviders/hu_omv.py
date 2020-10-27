# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_opening_hours, \
        clean_phone_to_str
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_omv(DataProvider):

    def constains(self):
        self.link = 'https://app.wigeogis.com/kunden/omvpetrom/data/getresults.php'
        self.tags = {'amenity': 'fuel', 'name': 'OMV', 'brand': 'OMV', 'fuel:diesel': 'yes',
                     'fuel:octane_95': 'yes', 'air_conditioning': 'yes', 'brand:wikidata': 'Q168238',
                     'brand:wikipedia': 'en:OMV', 'operator': 'OMV Hungária Kft.',
                     'operator:addr': '1117 Budapest, Október huszonharmadika utca 6-10 5. emelet 5/A.',
                     'ref:vatin:hu': '10542925-2-44', 'ref:vatin': 'HU10542925',
                     'ref:HU:company': '01-09-071584', 'contact:email': 'info.hungary@omv.com',
                     'contact:facebook': 'https://www.facebook.com/omvmagyarorszag',
                     'contact:fax': '+36 1 381 9899', 'contact:twitter': 'omv',
                     'contact:linkedin': 'https://www.linkedin.com/company/omv',
                     'contact:instagram': 'https://www.instagram.com/omv/',
                     'contact:youtube': 'https://www.youtube.com/user/omvofficial'}
        self.post = {'BRAND': 'OMV', 'CTRISO': 'HUN', 'MODE': 'NEXTDOOR', 'ANZ': '5',
                     'HASH': '23126a64295e2cf2a5e41f33fd4b0c416e09b0b8', 'TS': '1583951283'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        huomvfu = self.tags.copy()
        huomvfu.update(POS_HU_GEN)
        huomvfu.update(PAY_CASH)
        self.__types = [
            {'poi_code': 'huomvfu', 'poi_name': 'OMV', 'poi_type': 'fuel',
             'poi_tags': huomvfu, 'poi_url_base': 'https://www.omv.hu', 'poi_search_name': '(omv|omw|ömv|ömw|ovm|owm)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 450,
             'osm_search_distance_unsafe': 60},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype, self.post)
            if soup is not None:
                text = json.loads(soup)
                for poi_data in text:
                    try:
                        self.data.name = 'OMV'
                        self.data.code = 'huomvfu'
                        if poi_data.get('postcode') is not None and poi_data.get('postcode') != '':
                            self.data.postcode = poi_data.get(
                                'postcode').strip()
                        if poi_data.get('town_l') is not None and poi_data.get('town_l') != '':
                            self.data.city = clean_city(poi_data.get('town_l'))
                        if poi_data.get('open_hours') is not None:
                            oho, ohc = clean_opening_hours(
                                poi_data.get('open_hours'))
                            if oho == '00:00' and ohc == '24:00':
                                self.data.nonstop = True
                                self.data.public_holiday_open = True
                                oho, ohc = None, None
                            else:
                                self.data.public_holiday_open = False
                        else:
                            oho, ohc = None, None
                            self.data.public_holiday_open = False
                        for i in range(0, 7):
                            self.data.day_open(i, oho)
                            self.data.day_close(i, ohc)
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('y'), poi_data.get('x'))
                        if poi_data.get('address_l') is not None and poi_data.get('address_l') != '':
                            self.data.original = poi_data.get('address_l')
                            self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                                extract_street_housenumber_better_2(
                                    poi_data.get('address_l'))
                        if poi_data.get('telnr') is not None and poi_data.get('telnr') != '':
                            self.data.phone = clean_phone_to_str(
                                poi_data.get('telnr'))
                        self.data.fuel_octane_95 = True
                        self.data.fuel_diesel = True
                        self.data.fuel_octane_100 = True
                        self.data.fuel_diesel_gtl = True
                        self.data.compressed_air = True
                        self.data.add()
                    except Exception as e:
                        logging.error(e)
                        logging.error(poi_data)
                        logging.exception('Exception occurred')

        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')
