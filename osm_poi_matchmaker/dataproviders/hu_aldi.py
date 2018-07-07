# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import os
    import pandas as pd
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city
    from osm_poi_matchmaker.libs.opening_hours import OpeningHours
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_DATA = 'https://www.aldi.hu/hu/informaciok/informaciok/uezletkereso-es-nyitvatartas/'
POI_COMMON_TAGS = "'operator': 'ALDI Magyarország Élelmiszer Bt.', 'brand': 'Aldi', 'ref:vatin:hu':'22234663-2-44', 'wikipedia':'hu:Aldi', 'wikidata':'Q125054', 'facebook': 'https://www.facebook.com/ALDI.Magyarorszag', 'youtube':'https://www.youtube.com/user/ALDIMagyarorszag', 'instagram':'https://www.instagram.com/aldi.magyarorszag/', 'payment:mastercard': 'yes', 'payment:visa': 'yes'}"

class hu_aldi():

    def __init__(self, session, download_cache, prefer_osm_postcode, filename='hu_aldi.html'):
        self.session = session
        self.link = POI_DATA
        self.download_cache = download_cache
        self.prefer_osm_postcode = prefer_osm_postcode
        self.filename = filename

    @staticmethod
    def types():
        data = [{'poi_code': 'hualdisup', 'poi_name': 'Aldi', 'poi_type': 'shop',
                 'poi_tags': "{'shop': 'supermarket', " + POI_COMMON_TAGS + "}",
                 'poi_url_base': 'https://www.aldi.hu'}]
        return data

    def process(self):
        soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename))
        data = []
        insert_data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            table = soup.find('table', attrs={'class': 'contenttable is-header-top'})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [element.text.strip() for element in cols]
                data.append(cols)
            for poi_data in data:
                # Assign: code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber, ref, geom
                street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(poi_data[2])
                name = 'Aldi'
                code = 'hualdisup'
                postcode = poi_data[0].strip()
                city = clean_city(poi_data[1])
                branch = None
                website = None
                nonstop = None
                mo_o = None
                tu_o = None
                we_o = None
                th_o = None
                fr_o = None
                sa_o = None
                su_o = None
                mo_c = None
                tu_c = None
                we_c = None
                th_c = None
                fr_c = None
                sa_c = None
                su_c = None
                summer_mo_o = None
                summer_tu_o = None
                summer_we_o = None
                summer_th_o = None
                summer_fr_o = None
                summer_sa_o = None
                summer_su_o = None
                summer_mo_c = None
                summer_tu_c = None
                summer_we_c = None
                summer_th_c = None
                summer_fr_c = None
                summer_sa_c = None
                summer_su_c = None
                lunch_break_start = None
                lunch_break_stop = None
                t = OpeningHours(nonstop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c, fr_c, sa_c,
                                 su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o, summer_sa_o,
                                 summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c, summer_fr_c,
                                 summer_sa_c, summer_su_c, lunch_break_start, lunch_break_stop)
                opening_hours = t.process()
                original = poi_data[2]
                geom = None
                ref = None
                phone = None
                email = None
                insert_data.append(
                    [code, postcode, city, name, branch, website, original, street, housenumber, conscriptionnumber,
                     ref, phone, email, geom, nonstop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c,
                     fr_c, sa_c, su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o, summer_sa_o,
                     summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c,
                     summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunch_break_stop, opening_hours])
            if len(insert_data) < 1:
                logging.warning('Resultset is empty. Skipping ...')
            else:
                df = pd.DataFrame(insert_data)
                df.columns = POI_COLS
                insert_poi_dataframe(self.session, df)
