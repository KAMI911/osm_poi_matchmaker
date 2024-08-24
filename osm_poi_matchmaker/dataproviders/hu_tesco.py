# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone_to_str, \
        clean_url, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_osm_city_name_gpd
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_OTP, PAY_CASH
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_tesco(DataProvider):

    def contains(self):
        self.link = 'https://tesco.hu/Ajax?type=fetch-stores-for-area&reduceBy%5Btab%5D=all&bounds%5Bnw%5D%5Blat%5D=49.631214952216425&bounds%5Bnw%5D%5Blng%5D=11.727758183593778&bounds%5Bne%5D%5Blat%5D=49.631214952216425&bounds%5Bne%5D%5Blng%5D=27.004247441406278&bounds%5Bsw%5D%5Blat%5D=38.45256463471463&bounds%5Bsw%5D%5Blng%5D=11.727758183593778&bounds%5Bse%5D%5Blat%5D=38.45256463471463&bounds%5Bse%5D%5Blng%5D=27.004247441406278&currentCoords%5Blat%5D=44.30719090363816&currentCoords%5Blng%5D=19.366002812500028&instanceUUID=b5c4aa5f-9819-47d9-9e5a-d631e931c007'
        self.tags = {'operator': 'TESCO-GLOBAL Áruházak Zrt.',
                     'operator:addr': '2040 Budaörs, Kinizsi út 1-3.',
                     'ref:HU:company': '13-10-040628', 'ref:HU:vatin': '10307078-2-44',
                     'ref:vatin': 'HU10307078', 'brand': 'Tesco',
                     'brand:wikipedia': 'hu:Tesco',
                     'internet_access': 'wlan', 'internet_access:fee': 'no',
                     'internet_access:ssid': 'tesco-internet',
                     'contact:facebook': 'https://www.facebook.com/tescoaruhazak',
                     'contact:pinterest': 'https://www.pinterest.com/tescohungary/',
                     'contact:youtube': 'https://www.youtube.com/user/TescoMagyarorszag',
                     'loyalty_card': 'yes', 'payment:gift_card': 'yes', 'payment:wire_transfer': 'yes',
                     'air_conditioning': 'yes'}
        self.tags.update(POS_OTP)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hutescoexp = {'shop': 'convenience', 'brand:wikidata': 'Q98456772'}
        hutescoexp.update(self.tags)
        hutescoext = {'shop': 'supermarket',
                      'wheelchair': 'yes', 'source:wheelchair': 'website', 'brand:wikidata': 'Q25172225'}
        hutescoext.update(self.tags)
        hutescosup = {'shop': 'supermarket',
                      'wheelchair': 'yes', 'source:wheelchair': 'website', 'brand:wikidata': 'Q487494'}
        hutescosup.update(self.tags)
        husmrktexp = {'shop': 'convenience', 'alt_name': 'Tesco Expressz', 'brand:wikidata': 'Q487494'} # TODO: create wikidata tag
        husmrktexp.update(self.tags)
        husmrktsup = {'shop': 'supermarket', 'wheelchair': 'yes',
                      'source:wheelchair': 'website', 'alt_name': 'Tesco', 'brand:wikidata': 'Q487494'} # TODO: create wikidata tag
        husmrktsup.update(self.tags)
        self.__types = [
            {'poi_code': 'hutescoexp', 'poi_common_name': 'Tesco Expressz', 'poi_type': 'shop',
             'poi_tags': hutescoexp, 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
            {'poi_code': 'hutescoext', 'poi_common_name': 'Tesco Extra', 'poi_type': 'shop',
             'poi_tags': hutescoext, 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100},
            {'poi_code': 'hutescosup', 'poi_common_name': 'Tesco', 'poi_type': 'shop',
             'poi_tags': hutescosup, 'poi_url_base': 'https://tesco.hu', 'poi_search_name': 'tesco',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 1100},
            {'poi_code': 'husmrktexp', 'poi_common_name': 'S-Market', 'poi_type': 'shop',
             'poi_tags': husmrktexp, 'poi_url_base': 'https://tesco.hu',
             'poi_search_name': '(tesco|smarket|s-market|s market)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
            {'poi_code': 'husmrktsup', 'poi_common_name': 'S-Market', 'poi_type': 'shop',
             'poi_tags': husmrktsup, 'poi_url_base': 'https://tesco.hu',
             'poi_search_name': '(tesco|smarket|s-market|s market)',
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
                for poi_data in text.get('stores'):
                    try:
                        # Assign: code, postcode, city, name, branch, website, original, street, housenumber,
                        # conscriptionnumber, ref, geom
                        self.data.branch = clean_string(poi_data.get('store_name'))
                        self.data.ref = clean_string(poi_data.get('goldid'))
                        if clean_url(poi_data.get('urlname')) is not None:
                            self.data.website = 'https://tesco.hu/aruhazak/aruhaz/{}/'.format(clean_url(poi_data.get('urlname')))
                        opening = json.loads(poi_data.get('opening'))
                        for i in range(0, 7):
                            ind = str(i + 1) if i != 6 else '0'
                            if ind in opening:
                                self.data.day_open(i, opening[ind][0])
                                self.data.day_close(i, opening[ind][1])
                        self.data.lat, self.data.lon = check_hu_boundary(
                            poi_data.get('gpslat'), poi_data.get('gpslng'))
                        self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                            extract_street_housenumber_better_2(
                                poi_data.get('address'))
                        self.data.postcode = clean_string(poi_data.get('zipcode'))
                        self.data.city = clean_city(query_osm_city_name_gpd(
                            self.session, self.data.lat, self.data.lon))
                        if 'xpres' in poi_data.get('name'):
                            if self.data.city not in ['Győr', 'Sopron', 'Mosonmagyaróvár', 'Levél']:
                                self.data.code = 'hutescoexp'
                            else:
                                self.data.code = 'husmrktexp'
                        elif 'xtra' in poi_data.get('name'):
                            self.data.code = 'hutescoext'
                        else:
                            if self.data.city not in ['Levél']:
                                self.data.code = 'hutescosup'
                            else:
                                self.data.code = 'husmrktsup'
                        self.data.original = poi_data.get('address')
                        if poi_data.get('phone') is not None and poi_data.get('phone') != '':
                            self.data.phone = clean_phone_to_str(
                                poi_data.get('phone'))
                        if poi_data.get('goldid') is not None and poi_data.get('goldid') != '':
                            self.data.ref = poi_data.get('goldid').strip()
                        self.data.public_holiday_open = False
                        self.data.add()
                    except Exception as e:
                        logging.exception('Exception occurred: {}'.format(e))
                        logging.exception(traceback.print_exc())
                        logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.print_exc())
