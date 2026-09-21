# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import pandas as pd
    import numpy as np
    from lxml import etree
    from osm_poi_matchmaker.dao.data_handlers import insert_city_dataframe, insert_street_type_dataframe, insert_patch_data_dataframe, insert_country_data_dataframe
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
    from osm_poi_matchmaker.libs.xml import save_downloaded_xml
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POI_COLS_CITY = ['city_post_code', 'city_name']
POI_COLS_STREET_TYPE = ['street_type']


def _safe_parse_xml(xml):
    """etree.fromstring(), returning None instead of raising on malformed input -
    e.g. Magyar Posta's feeds occasionally serve an HTML "under maintenance" page
    (HTTP 200, so save_downloaded_xml() has no reason to treat it as a failed
    download) in place of the real XML.

    :param xml: Raw XML content (bytes or str), or None.
    :return: The parsed root Element, or None if xml is None or doesn't parse.
    """
    if xml is None:
        return None
    try:
        return etree.fromstring(xml)
    except Exception as e:
        logging.warning('XML did not parse: {}'.format(e))
        return None


class hu_city_postcode():
    """Imports Hungarian city/postcode data from an Excel workbook (used for initial seeding, superseded by hu_city_postcode_from_xml)."""

    def __init__(self, session, link):
        self.session = session
        self.link = link

    def process(self):
        xl = pd.ExcelFile(self.link)
        df = xl.parse("Települések")
        del df['Településrész']
        insert_city_dataframe(self.session, df)
        big_cities = [['Budapest', 'Bp.u.'],
                      ['Miskolc', 'Miskolc u.'],
                      ['Debrecen', 'Debrecen u.'],
                      ['Szeged', 'Szeged u.'],
                      ['Pécs', 'Pécs u.'],
                      ['Győr', 'Győr u.']
                      ]
        for city, sheet in big_cities:
            df = xl.parse(sheet)
            df.columns.values[0] = 'city_post_code'
            df['city_name'] = city
            df = df[['city_post_code', 'city_name']]
            df.drop_duplicates('city_post_code', keep='first', inplace=True)
            insert_city_dataframe(self.session, df)


class hu_city_postcode_from_xml():
    """Imports Hungarian city/postcode data from Hungarian Post's ZipCodes.xml feed into the city table."""

    def __init__(self, session, link, download_cache, filename='hu_city_postcode.xml'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def process(self):
        cache_path = os.path.join(self.download_cache, self.filename)
        # save_downloaded_xml() already falls back to this file's previously cached
        # content (without overwriting it) if the live fetch fails or comes back
        # malformed - see libs/xml.py. root is still re-parsed here in case the
        # cached copy itself is missing/corrupt for some other reason.
        xml = save_downloaded_xml('{}'.format(self.link), cache_path)
        root = _safe_parse_xml(xml)
        if root is None:
            logging.error('No usable city/postcode data (live fetch failed and no valid '
                          'cached copy exists) - skipping this update. Existing city data '
                          'from a previous run, if any, is left untouched in the database.')
            return

        insert_data = []
        for e in root.findall('zipCode'):
            cities = e[1].text
            postcode = e[0].text.strip()
            for i in cities.split('|'):
                insert_data.append([postcode, i.strip()])
        df = pd.DataFrame(insert_data)
        df.columns = POI_COLS_CITY
        insert_city_dataframe(self.session, df)


class hu_street_types_from_xml():
    """Imports the list of valid Hungarian street-type suffixes from Hungarian Post's StreetTypes.xml feed."""

    def __init__(self, session, link, download_cache, filename='hu_street_types.xml'):
        self.session = session
        self.link = link
        self.download_cache = download_cache
        self.filename = filename

    def process(self):
        cache_path = os.path.join(self.download_cache, self.filename)
        # See hu_city_postcode_from_xml.process() - save_downloaded_xml() already
        # handles the fallback-to-previous-cache dance.
        xml = save_downloaded_xml('{}'.format(self.link), cache_path)
        root = _safe_parse_xml(xml)
        if root is None:
            logging.error('No usable street-type data (live fetch failed and no valid '
                          'cached copy exists) - skipping this update. Existing street_type '
                          'data from a previous run, if any, is left untouched in the database.')
            return

        insert_data = []
        for e in root.findall('streetType'):
            if e.text is not None:
                street_type = e.text.strip()
                insert_data.append(street_type)
        df = pd.DataFrame(insert_data)
        df.columns = POI_COLS_STREET_TYPE
        insert_street_type_dataframe(self.session, df)


class poi_patch_from_csv():
    """Insert CVS patch data into database poi_patch table
    """

    def __init__(self, session, filename: str = 'poi_patch.tsv'):
        """[summary]

        Args:
            session ([type]): [description]
            filename (str, optional): Filename of CSV file to import. Defaults to 'poi_patch.tsv'.
        """
        self.session = session
        self.filename = filename

    def process(self):
        csv = save_downloaded_pd(None, os.path.join('data', self.filename), None)
        csv = csv.replace({np.nan: None})
        insert_patch_data_dataframe(self.session, csv)


class poi_country_from_csv():
    """Insert CVS country data into database country table
    """

    def __init__(self, session, filename: str = 'country.tsv'):
        """[summary]

        Args:
            session ([type]): [description]
            filename (str, optional): Filename of CSV file to import. Defaults to 'country.tsv'.
        """
        self.session = session
        self.filename = filename

    def process(self):
        csv = save_downloaded_pd(None, os.path.join('data', self.filename), None)
        csv = csv.replace({np.nan: None})
        insert_country_data_dataframe(self.session, csv)
