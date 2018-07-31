# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import re
    import json
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_javascript_variable, clean_opening_hours
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from osm_poi_matchmaker.utils.enums import WeekDaysLong
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_DATA = 'https://www.rossmann.hu/uzletkereso'


class hu_rossmann():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_rossmann.html', **kwargs):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename
        if 'verify_link' in kwargs:
            self.verify_link = kwargs['verify_link']
        else:
            self.verify_link = None

    @staticmethod
    def types():
        data = [{'poi_code': 'hurossmche', 'poi_name': 'Rossmann', 'poi_type': 'chemist',
                 'poi_tags': "{'shop': 'chemist', 'operator': 'Rossmann Magyarország Kft.', 'brand':'Rossmann', 'addr:country': 'HU', 'facebook':'https://www.facebook.com/Rossmann.hu', 'youtube': 'https://www.youtube.com/channel/UCmUCPmvMLL3IaXRBtx7-J7Q', 'instagram':'https://www.instagram.com/rossmann_hu', 'payment:cash': 'yes', 'payment:debit_cards': 'yes'}",
                 'poi_url_base': 'https://www.rossmann.hu', 'poi_search_name': 'rossmann'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename), None,
                                    self.verify_link)
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('^\s*var\s*places.*')
            script = soup.find('script', text=pattern)
            m = pattern.match(script.get_text())
            data = m.group(0)
            data = clean_javascript_variable(data, 'places')
            text = json.loads(data)
            data = POIDataset()
            for poi_data in text:
                poi_data = poi_data['addresses'][0]
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                data.street, data.housenumber, data.conscriptionnumber = extract_street_housenumber_better_2(
                    poi_data['address'])
                data.name = 'Rossmann'
                data.code = 'hurossmche'
                data.city = clean_city(poi_data['city'])
                data.postcode = poi_data['zip'].strip()
                for i in range(0, 7):
                    if poi_data['business_hours'][WeekDaysLong(i).name.lower()] is not None:
                        opening, closing = clean_opening_hours(poi_data['business_hours'][WeekDaysLong(i).name.lower()])
                        data.day_open_close(i, opening, closing)
                    else:
                        data.day_open_close(i, None, None)
                data.lat, data.lon = check_hu_boundary(poi_data['position'][0], poi_data['position'][1])
                data.postcode = query_postcode_osm_external(self.prefer_osm_postcode, self.session, data.lat, data.lon, data.postcode)
                data.original = poi_data['address']
                data.public_holiday_open = False
                data.add()
            if data.lenght() < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                insert_poi_dataframe(self.session, data.process())
