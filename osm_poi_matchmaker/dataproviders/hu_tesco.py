# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_phone, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external, query_osm_city_name_gpd
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'https://tesco.hu/Ajax?type=fetch-stores-for-area&reduceBy%5Btab%5D=all&bounds%5Bnw%5D%5Blat%5D=49.631214952216425&bounds%5Bnw%5D%5Blng%5D=11.727758183593778&bounds%5Bne%5D%5Blat%5D=49.631214952216425&bounds%5Bne%5D%5Blng%5D=27.004247441406278&bounds%5Bsw%5D%5Blat%5D=38.45256463471463&bounds%5Bsw%5D%5Blng%5D=11.727758183593778&bounds%5Bse%5D%5Blat%5D=38.45256463471463&bounds%5Bse%5D%5Blng%5D=27.004247441406278&currentCoords%5Blat%5D=44.30719090363816&currentCoords%5Blng%5D=19.366002812500028&instanceUUID=b5c4aa5f-9819-47d9-9e5a-d631e931c007'
POI_COMMON_TAGS = "'operator': 'TESCO-GLOBAL Áruházak Zrt.', 'ref:vatin:hu': '10307078-2-44', 'brand': 'Tesco', 'brand:wikipedia': 'hu:Tesco', 'brand:wikidata': 'Q487494', 'addr:country': 'HU', 'internet_access': 'wlan', 'internet_access:fee': 'no', 'facebook': 'https://www.facebook.com/tescoaruhazak', 'youtube': 'https://www.youtube.com/user/TescoMagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'air_conditioning': 'yes'"


class hu_tesco():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_tesco.html'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hutescoexp', 'poi_name': 'Tesco Expressz', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.tesco.hu', 'poi_search_name': 'tesco'},
                {'poi_code': 'hutescoext', 'poi_name': 'Tesco Extra', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'wheelchair': 'yes', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.tesco.hu', 'poi_search_name': 'tesco'},
                {'poi_code': 'hutescosup', 'poi_name': 'Tesco', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'wheelchair': 'yes', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.tesco.hu', 'poi_search_name': 'tesco'},
                {'poi_code': 'husmrktexp', 'poi_name': 'S-Market', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', 'alt_name': 'Tesco Expressz', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.tesco.hu', 'poi_search_name': '(tesco|smarket|s-market|s market)'},
                {'poi_code': 'husmrktsup', 'poi_name': 'S-Market', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', 'wheelchair': 'yes', 'alt_name': 'Tesco', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.tesco.hu', 'poi_search_name': '(tesco|smarket|s-market|s market)'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            # script = soup.find('div', attrs={'data-stores':True})
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text['stores']:
                try:
                    print(poi_data)
                    # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                    data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                        poi_data['address'])
                    data.branch = poi_data['store_name']
                    data.website = 'https://tesco.hu/aruhazak/aruhaz/{}/'.format(poi_data['urlname'])
                    opening = json.loads(poi_data['opening'])
                    for i in range(0, 7):
                        ind = str(i + 1) if i != 6 else '0'
                        if ind in opening:
                            data.day_open(i, opening[ind][0])
                            data.day_close(i, opening[ind][1])
                    data.lat, data.lon = check_hu_boundary(poi_data['gpslat'], poi_data['gpslng'])
                    data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon,
                                                                None)
                    data.city = query_osm_city_name_gpd(self.session, data.lat, data.lon)
                    if 'xpres' in poi_data['name']:
                        if data.city not in ['Győr', 'Sopron', 'Mosonmagyaróvár', 'Levél']:
                            data.name = 'Tesco Expressz'
                            data.code = 'hutescoexp'
                        else:
                            data.name = 'S-Market'
                            data.code = 'husmrktexp'
                    elif 'xtra' in poi_data['name']:
                        data.name = 'Tesco Extra'
                        data.code = 'hutescoext'
                    else:
                        if data.city not in ['Győr', 'Sopron', 'Mosonmagyaróvár', 'Levél']:
                            data.name = 'Tesco'
                            data.code = 'hutescosup'
                        else:
                            data.name = 'S-Market'
                            data.code = 'husmrktsup'
                    data.original = poi_data['address']
                    if 'phone' in poi_data and poi_data['phone'] != '':
                        data.phone = clean_phone(poi_data['phone'])
                    data.public_holiday_open = False
                    data.add()
                except Exception as err:
                    print (err)
                    traceback.print_exc()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
