# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import os
    from lxml import etree
    from osm_poi_matchmaker.libs.xml import save_downloaded_xml
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class hu_mol_bubi(DataProvider):
    # Processing https://bubi.nextbike.net/maps/nextbike-live.xml?&domains=mb file


    def constains(self):
        self.link = 'https://bubi.nextbike.net/maps/nextbike-live.xml?&domains=mb'
        self.POI_COMMON_TAGS = ""
        self.filetype = FileType.xml
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        self.__types = [{'poi_code': 'hububibir', 'poi_name': 'MOL Bubi', 'poi_type': 'bicycle_rental',
                 'poi_tags': "{'amenity': 'bicycle_rental', 'brand': 'MOL Bubi', 'operator': 'BKK MOL Bubi', 'network': 'bubi',  'contact:instagram': 'https://www.instagram.com/molbubi/', 'contact:facebook': 'https://www.facebook.com/molbubi', 'contact:youtube': 'https://www.youtube.com/user/bkkweb', 'twitter': 'molbubi'}",
                 'poi_url_base': 'https://molbubi.bkk.hu', 'poi_search_name': '(mol bubi|bubi)'}]
        return self.__types


    def process(self):
        try:
            xml = save_downloaded_xml('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            root = etree.fromstring(xml)
            for pla in root.iter('place'):
                try:
                    self.data.name = 'MOL Bubi'
                    self.data.code = 'hububibir'
                    self.data.city = 'Budapest'
                    if pla.attrib.get('name') is not None and pla.attrib.get('name') != '':
                        self.data.branch = pla.attrib.get('name').split('-')[1].strip() \
                            if pla.attrib.get('name') is not None else None
                        self.data.ref = pla.attrib['name'].split('-')[0].strip() \
                            if pla.attrib['name'] is not None else None
                    self.data.nonstop = True
                    # self.data.capacity = pla.attrib['bike_racks'].strip() \
                    # if pla.attrib['bike_racks'] is not None else None
                    self.data.lat, self.data.lon = \
                        check_hu_boundary(pla.attrib.get('lat').replace(',', '.'),
                                          pla.attrib.get('lng').replace(',', '.'))
                    self.data.postcode = query_postcode_osm_external(True, self.session, self.data.lon,
                                                                     self.data.lat, None)
                    self.data.public_holiday_open = True
                    self.data.add()
                except Exception as e:
                    logging.error(e)
                    logging.error(pla)
                    logging.error(traceback.print_exc())
        except Exception as e:
            logging.error(e)
            logging.error(traceback.print_exc())
