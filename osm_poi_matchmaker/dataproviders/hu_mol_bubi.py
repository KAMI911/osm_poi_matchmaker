# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import json
    import pandas as pd
    from lxml import etree
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.xml import save_downloaded_xml
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.geo import check_geom, check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'https://bubi.nextbike.net/maps/nextbike-live.xml?&domains=mb'


class hu_mol_bubi():
    # Processing https://bubi.nextbike.net/maps/nextbike-live.xml?&domains=mb file
    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_mol_bubi.xml'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hububibir', 'poi_name': 'MOL Bubi', 'poi_type': 'bicycle_rental',
                 'poi_tags': "{'amenity': 'bicycle_rental', 'brand': 'MOL Bubi', 'operator': 'BKK MOL Bubi', 'network': 'bubi', 'addr:country': 'HU', 'payment:credit_cards': 'yes', 'facebook': 'https://www.facebook.com/molbubi/', 'youtube': 'https://www.youtube.com/user/bkkweb'}",
                 'poi_url_base': 'https://molbubi.bkk.hu/'}]
        return data

    def process(self):
        xml = save_downloaded_xml('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        root = etree.fromstring(xml)
        data = POIDataset()
        for e in root.iter('place'):
            data.name = 'MOL Bubi'
            data.code = 'hububibir'
            data.city = 'Budapest'
            data.branch = e.attrib['name'].split('-')[1].strip() if e.attrib['name'] is not None else None
            data.ref = e.attrib['name'].split('-')[0].strip() if e.attrib['name'] is not None else None
            data.nonstop = True
            # data.capacity = e.attrib['bike_racks'].strip() if e.attrib['bike_racks'] is not None else None
            data.lat, data.lon = check_hu_boundary(e.attrib['lat'].replace(',', '.'), e.attrib['lng'].replace(',', '.'))
            data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, None)
            data.add()
        if data.lenght() < 1:
            logging.warning('Resultset is empty. Skipping ...')
        else:
            insert_poi_dataframe(self.session, data.process())
