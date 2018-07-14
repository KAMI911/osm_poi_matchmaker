# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'https://www.spar.hu/bin/aspiag/storefinder/stores?country=HU'
POI_COMMON_TAGS = "'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'brand:wikipedia': 'hu:Spar ', 'brand:wikidata': 'Q610492', 'addr:country': 'HU', 'email': 'vevoszolgalat@spar.hu', 'phone': '+36208237727', 'facebook': 'https://www.facebook.com/sparmagyarorszag', 'youtube': 'https://www.youtube.com/channel/UC9tu8COHiy4WkeTIN1k_Y8A', 'instagram': 'https://www.instagram.com/sparmagyarorszag', 'payment:cash': 'yes', 'payment:debit_cards': 'yes',"
PATTERN_SPAR_REF = re.compile('\((.*?)\)')


class hu_spar():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_spar.json'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'husparexp', 'poi_name': 'Spar Expressz', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'convenience', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': 'spar'},
                {'poi_code': 'husparint', 'poi_name': 'Interspar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': 'spar'},
                {'poi_code': 'husparsup', 'poi_name': 'Spar', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.spar.hu', 'poi_search_name': 'spar'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        if soup != None:
            text = json.loads(soup.get_text())
            data = POIDataset()
            for poi_data in text:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                if 'xpres' in poi_data['name']:
                    data.name = 'Spar Expressz'
                    data.code = 'husparexp'
                elif 'INTER' in poi_data['name']:
                    data.name = 'Interspar'
                    data.code = 'husparint'
                elif 'market' in poi_data['name']:
                    data.name = 'Spar'
                    data.code = 'husparsup'
                else:
                    data.name = 'Spar'
                    data.code = 'husparsup'
                poi_data['name'] = poi_data['name'].replace('INTERSPAR', 'Interspar')
                poi_data['name'] = poi_data['name'].replace('SPAR', 'Spar')
                ref_match = PATTERN_SPAR_REF.search(poi_data['name'])
                data.ref = ref_match.group(1).strip() if ref_match is not None else None
                data.city = clean_city(poi_data['city'])
                data.postcode = poi_data['zipCode'].strip()
                data.branch = poi_data['name'].split('(')[0].strip()
                data.website = poi_data['pageUrl'].strip()
                data.lat, data.lon = check_hu_boundary(poi_data['latitude'], poi_data['longitude'])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                data.original = poi_data['address']
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
