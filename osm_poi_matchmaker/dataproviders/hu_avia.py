# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import os
    import re
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_all_address, clean_javascript_variable, clean_phone_to_str, \
        clean_email
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_avia(DataProvider):


    def constains(self):
        self.link = 'https://www.avia.hu/kapcsolat/toltoallomasok'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'html'

    def types(self):
        self.__type = [{'poi_code': 'huaviafu', 'poi_name': 'Avia', 'poi_type': 'fuel',
                 'poi_tags': "{'amenity': 'fuel', 'brand': 'Avia', 'operator': 'AVIA Hungária Kft.', " + POS_HU_GEN + PAY_CASH + "'fuel:diesel': 'yes', 'fuel:octane_95': 'yes', 'contact:email': 'avia@avia.hu', 'contact:facebook': 'https://www.facebook.com/AVIAHungary', 'contact:youtube': 'https://www.youtube.com/channel/UCjvjkjf2RgmKBuTnKSXk-Rg', }",
                 'poi_url_base': 'https://www.avia.hu', 'poi_search_name': 'avia', 'osm_search_distance_perfect': 30000, 'osm_search_distance_safe': 800, 'osm_search_distance_unsafe': 110}]
        return self.__type

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                pattern = re.compile('var\s*markers\s*=\s*((.*\n)*\]\;)', re.MULTILINE)
                script = soup.find('script', text=pattern)
                m = pattern.search(script.get_text())
                data = m.group(0)
                data = data.replace("'", '"')
                data = clean_javascript_variable(data, 'markers')
                text = json.loads(data)
                for poi_data in text:
                    self.data.name = 'Avia'
                    self.data.code = 'huaviafu'
                    if self.data.city is None:
                        self.data.city = poi_data['title']
                    self.data.ref = poi_data['kutid'] if poi_data['kutid'] is not None and poi_data['kutid'] != '' else None
                    self.data.lat, self.data.lon = check_hu_boundary(poi_data['lat'], poi_data['lng'])
                    if poi_data['cim'] is not None and poi_data['cim'] != '':
                        self.data.postcode, self.data.city, self.data.street, self.data.housenumber, self.data.conscriptionnumber = extract_all_address(
                            poi_data['cim'])
                    self.data.website = '/toltoallomas/?id={}'.format(str(poi_data['kutid'])) if poi_data[
                                                                                                'kutid'] is not None and \
                                                                                            poi_data[
                                                                                                'kutid'] != '' else None
                    self.data.original = poi_data['cim']
                    if 'tel' in poi_data and poi_data['tel'] != '':
                        self.data.phone = clean_phone_to_str(poi_data['tel'])
                    else:
                        self.data.phone = None
                    if 'email' in poi_data and poi_data['email'] != '':
                        self.data.email = clean_email(poi_data['email'])
                    else:
                        self.data.email = None
                    self.data.public_holiday_open = False
                    self.data.fuel_octane_95 = True if poi_data.get('b95') == '1' or poi_data.get('b95g') == '1' else False
                    self.data.fuel_diesel = True if poi_data.get('dies') == '1' or poi_data.get('gdies') == '1' else False
                    self.data.fuel_octane_98 = True if poi_data.get('b98') == '1' else False
                    self.data.fuel_lpg = True if poi_data.get('lpg') == '1' else False
                    self.data.fuel_e85 = True if poi_data.get('e85') == '1' else False
                    self.data.rent_lpg_bottles = True if poi_data.get('pgaz') == '1' else False
                    self.data.compressed_air = True if poi_data.get('komp') == '1' else False
                    self.data.restaurant = True if poi_data.get('etterem') == '1' else False
                    self.data.food = True if poi_data.get('bufe') == '1' else False
                    self.data.truck = True if poi_data.get('kpark') == '1' else False
                    self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
