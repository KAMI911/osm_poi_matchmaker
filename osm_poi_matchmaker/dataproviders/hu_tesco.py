# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external, query_osm_city_name_gpd
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_OTP, PAY_CASH
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_tesco(DataProvider):


    def constains(self):
        self.link = 'https://tesco.hu/Ajax?type=fetch-stores-for-area&reduceBy%5Btab%5D=all&bounds%5Bnw%5D%5Blat%5D=49.631214952216425&bounds%5Bnw%5D%5Blng%5D=11.727758183593778&bounds%5Bne%5D%5Blat%5D=49.631214952216425&bounds%5Bne%5D%5Blng%5D=27.004247441406278&bounds%5Bsw%5D%5Blat%5D=38.45256463471463&bounds%5Bsw%5D%5Blng%5D=11.727758183593778&bounds%5Bse%5D%5Blat%5D=38.45256463471463&bounds%5Bse%5D%5Blng%5D=27.004247441406278&currentCoords%5Blat%5D=44.30719090363816&currentCoords%5Blng%5D=19.366002812500028&instanceUUID=b5c4aa5f-9819-47d9-9e5a-d631e931c007'
        self.POI_COMMON_TAGS = "'operator': 'TESCO-GLOBAL Áruházak Zrt.', 'operator:addr': '2040 Budaörs, Kinizsi út 1-3.'," \
                               " 'ref:HU:company': '13-10-040628', 'ref:vatin:hu': '10307078-2-44', 'ref:vatin': 'HU10307078', 'brand': 'Tesco'," \
                               " 'brand:wikipedia': 'hu:Tesco', 'brand:wikidata': 'Q487494', " \
                               " 'internet_access': 'wlan', 'internet_access:fee': 'no', 'internet_access:ssid': 'tesco-internet'," \
                               " 'contact:facebook': 'https://www.facebook.com/tescoaruhazak', 'contact:pinterest': 'https://www.pinterest.com/tescohungary/'," \
                               " 'contact:youtube': 'https://www.youtube.com/user/TescoMagyarorszag',  'loyalty_card': 'yes'," \
                               + POS_OTP + PAY_CASH + "'payment:gift_card': 'yes', 'payment:wire_transfer': 'yes'," \
                               " 'air_conditioning': 'yes'"
        self.filename = self.filename + 'json'

    def types(self):
        self.__types = [{'poi_code': 'hutescoexp', 'poi_name': 'Tesco Expressz', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
                {'poi_code': 'hutescoext', 'poi_name': 'Tesco Extra', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'wheelchair': 'yes', 'source:wheelchair': 'website', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100},
                {'poi_code': 'hutescosup', 'poi_name': 'Tesco', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'wheelchair': 'yes', 'source:wheelchair': 'website',  " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100},
                {'poi_code': 'husmrktexp', 'poi_name': 'S-Market', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', 'alt_name': 'Tesco Expressz', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://tesco.hu', 'poi_search_name': '(tesco|smarket|s-market|s market)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
                {'poi_code': 'husmrktsup', 'poi_name': 'S-Market', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'wheelchair': 'yes', 'source:wheelchair': 'website',  'alt_name': 'Tesco', " + self.POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://tesco.hu', 'poi_search_name': '(tesco|smarket|s-market|s market)', 'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                # script = soup.find('div', attrs={'data-stores':True})
                text = json.loads(soup.get_text())
                for poi_data in text['stores']:
                    try:
                        # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                        self.data.branch = poi_data['store_name']
                        self.data.ref = poi_data['goldid']
                        self.data.website = 'https://tesco.hu/aruhazak/aruhaz/{}/'.format(poi_data['urlname'])
                        opening = json.loads(poi_data['opening'])
                        for i in range(0, 7):
                            ind = str(i + 1) if i != 6 else '0'
                            if ind in opening:
                                self.data.day_open(i, opening[ind][0])
                                self.data.day_close(i, opening[ind][1])
                        self.data.lat, self.data.lon = check_hu_boundary(poi_data['gpslat'], poi_data['gpslng'])
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_street_housenumber_better_2(poi_data['address'])
                        self.data.postcode = poi_data.get('zipcode').strip()
                        self.data.city = query_osm_city_name_gpd(self.session, self.data.lat, self.data.lon)
                        if 'xpres' in poi_data['name']:
                            if self.data.city not in ['Győr', 'Sopron', 'Mosonmagyaróvár', 'Levél']:
                                self.data.name = 'Tesco Expressz'
                                self.data.code = 'hutescoexp'
                            else:
                                self.data.name = 'S-Market'
                                self.data.code = 'husmrktexp'
                        elif 'xtra' in poi_data['name']:
                            self.data.name = 'Tesco Extra'
                            self.data.code = 'hutescoext'
                        else:
                            if self.data.city not in ['Levél']:
                                self.data.name = 'Tesco'
                                self.data.code = 'hutescosup'
                            else:
                                self.data.name = 'S-Market'
                                self.data.code = 'husmrktsup'
                        self.data.original = poi_data['address']
                        if 'phone' in poi_data and poi_data['phone'] != '':
                            self.data.phone = clean_phone_to_str(poi_data['phone'])
                        if 'goldid' in poi_data and poi_data['goldid'] != '':
                            self.data.ref = poi_data['goldid'].strip()
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as err:
                        logging.error(err)
                        logging.error(traceback.print_exc())
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
