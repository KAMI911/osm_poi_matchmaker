# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception("Exception occurred")
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
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            for pla in soup.findAll('place'):
                try:
                    self.data.name = 'MOL Bubi'
                    self.data.code = 'hububibir'
                    self.data.city = 'Budapest'
                    if pla.get('name') is not None and pla.get('name') != '':
                        self.data.branch = pla.get('name').split('-')[1].strip() \
                            if pla.get('name') is not None else None
                        self.data.ref = pla.get('name').split('-')[0].strip() \
                            if pla.get('name') is not None else None
                    self.data.nonstop = True
                    # self.data.capacity = pla.attrib['bike_racks'].strip() \
                    # if pla.attrib['bike_racks'] is not None else None
                    self.data.lat, self.data.lon = \
                        check_hu_boundary(pla.get('lat').replace(',', '.'),
                                          pla.get('lng').replace(',', '.'))
                    self.data.postcode = query_postcode_osm_external(True, self.session, self.data.lon,
                                                                     self.data.lat, None)
                    self.data.public_holiday_open = True
                    self.data.add()
                except Exception as e:
                    logging.error(e)
                    logging.error(pla)
                    logging.exception("Exception occurred")
        except Exception as e:
            logging.error(e)
            logging.exception("Exception occurred")
            logging.error(soup)