# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    from lxml import etree
    from dao.data_handlers import insert_poi_dataframe
    from libs.xml import save_downloaded_xml
    from libs.address import clean_city
    from libs.geo import check_hu_boundary
    from libs.osm import query_postcode_osm_external
    from libs.poi_dataset import POIDataset
    from utils.data_provider import DataProvider
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


class hu_mol_bubi(DataProvider):
    # Processing https://bubi.nextbike.net/maps/nextbike-live.xml?&domains=mb file


    def constains(self):
        self.link = 'https://bubi.nextbike.net/maps/nextbike-live.xml?&domains=mb'
        self.POI_COMMON_TAGS = ""
        self.filename = self.filename + 'xml'

    def types(self):
        self.__types = [{'poi_code': 'hububibir', 'poi_name': 'MOL Bubi', 'poi_type': 'bicycle_rental',
                 'poi_tags': "{'amenity': 'bicycle_rental', 'brand': 'MOL Bubi', 'operator': 'BKK MOL Bubi', 'network': 'bubi',  'contact:instagram': 'https://www.instagram.com/molbubi/', 'contact:facebook': 'https://www.facebook.com/molbubi', 'contact:youtube': 'https://www.youtube.com/user/bkkweb', 'twitter': 'molbubi'}",
                 'poi_url_base': 'https://molbubi.bkk.hu', 'poi_search_name': '(mol bubi|bubi)'}]
        return self.__types


    def process(self):
        try:
            xml = save_downloaded_xml('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
            root = etree.fromstring(xml)
            data = POIDataset()
            for e in root.iter('place'):
                self.data.name = 'MOL Bubi'
                self.data.code = 'hububibir'
                self.data.city = 'Budapest'
                self.data.branch = e.attrib['name'].split('-')[1].strip() if e.attrib['name'] is not None else None
                self.data.ref = e.attrib['name'].split('-')[0].strip() if e.attrib['name'] is not None else None
                self.data.nonstop = True
                # self.data.capacity = e.attrib['bike_racks'].strip() if e.attrib['bike_racks'] is not None else None
                self.data.lat, self.data.lon = check_hu_boundary(e.attrib['lat'].replace(',', '.'), e.attrib['lng'].replace(',', '.'))
                self.data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, self.data.lat, self.data.lon,
                                                            None)
                self.data.public_holiday_open = True
                self.data.add()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(e)
