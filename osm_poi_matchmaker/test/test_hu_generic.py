# -*- coding: utf-8 -*-

"""Regression tests for hu_generic.py's hu_city_postcode_from_xml /
hu_street_types_from_xml, covering a real production crash: Magyar Posta's
ZipCodes.xml/StreetTypes.xml feeds occasionally serve an HTML "under
maintenance" page (a valid HTTP 200, so the download layer used to have no
reason to treat it as a failed download) instead of the real XML. Before the
fix, root = etree.fromstring(xml) raised inside a try/except that left `root`
unbound, and the very next line (root.findall(...)) raised an uncaught
UnboundLocalError that aborted the entire pipeline at STAGE 0, before any of
the ~90 data providers could even begin harvesting.

The fallback-to-previously-cached-data behavior itself now lives in
libs/xml.py's save_downloaded_xml() (see test_xml.py) - these tests cover
hu_generic.py's own contract with that dependency: valid content gets
inserted, and None (whatever save_downloaded_xml() returns when neither a
fresh nor a cached copy is usable) is handled by skipping gracefully instead
of raising.
"""

try:
    import unittest
    import logging
    import sys
    from unittest.mock import MagicMock, patch
    from osm_poi_matchmaker.dataproviders.hu_generic import hu_city_postcode_from_xml, hu_street_types_from_xml, _safe_parse_xml
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

MAINTENANCE_HTML = (b'<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN">'
                     b'<html><head><title>Informatikai karbantartas</title></head>'
                     b'<body>down</body></html>')

GOOD_CITY_XML = (b'<?xml version="1.0" encoding="UTF-8"?>'
                  b'<zipCodes><zipCode><postCode>1011</postCode><cities>Budapest</cities></zipCode></zipCodes>')

GOOD_STREET_TYPE_XML = (b'<?xml version="1.0" encoding="UTF-8"?>'
                          b'<streetTypes><streetType>utca</streetType><streetType>ter</streetType></streetTypes>')


class TestSafeParseXml(unittest.TestCase):

    def test_none_returns_none(self):
        self.assertIsNone(_safe_parse_xml(None))

    def test_malformed_returns_none_instead_of_raising(self):
        self.assertIsNone(_safe_parse_xml(MAINTENANCE_HTML))

    def test_valid_xml_parses(self):
        root = _safe_parse_xml(GOOD_CITY_XML)
        self.assertIsNotNone(root)
        self.assertEqual(len(root.findall('zipCode')), 1)

    def test_xml_declaration_with_encoding_does_not_raise(self):
        # etree.fromstring() rejects a `str` containing an `encoding=` XML
        # declaration - bytes must be used, which is what save_downloaded_xml()
        # hands in.
        root = _safe_parse_xml(GOOD_CITY_XML)
        self.assertIsNotNone(root)


class TestCityPostcodeProcess(unittest.TestCase):

    def test_good_xml_inserts_normally(self):
        with patch('osm_poi_matchmaker.dataproviders.hu_generic.save_downloaded_xml', return_value=GOOD_CITY_XML), \
             patch('osm_poi_matchmaker.dataproviders.hu_generic.insert_city_dataframe') as mock_insert:
            inst = hu_city_postcode_from_xml(session=MagicMock(), link='http://example', download_cache='/tmp')
            inst.process()

        self.assertTrue(mock_insert.called)
        df = mock_insert.call_args[0][1]
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['city_name'], 'Budapest')

    def test_none_from_download_layer_skips_without_raising(self):
        # save_downloaded_xml() returns None when neither a fresh download nor any
        # cached copy is usable (see test_xml.py) - process() must not crash on it.
        with patch('osm_poi_matchmaker.dataproviders.hu_generic.save_downloaded_xml', return_value=None), \
             patch('osm_poi_matchmaker.dataproviders.hu_generic.insert_city_dataframe') as mock_insert:
            inst = hu_city_postcode_from_xml(session=MagicMock(), link='http://example', download_cache='/tmp')
            inst.process()  # must not raise UnboundLocalError

        self.assertFalse(mock_insert.called)


class TestStreetTypesProcess(unittest.TestCase):

    def test_good_xml_inserts_normally(self):
        with patch('osm_poi_matchmaker.dataproviders.hu_generic.save_downloaded_xml', return_value=GOOD_STREET_TYPE_XML), \
             patch('osm_poi_matchmaker.dataproviders.hu_generic.insert_street_type_dataframe') as mock_insert:
            inst = hu_street_types_from_xml(session=MagicMock(), link='http://example', download_cache='/tmp')
            inst.process()

        self.assertTrue(mock_insert.called)
        df = mock_insert.call_args[0][1]
        self.assertEqual(sorted(df['street_type']), ['ter', 'utca'])

    def test_none_from_download_layer_skips_without_raising(self):
        with patch('osm_poi_matchmaker.dataproviders.hu_generic.save_downloaded_xml', return_value=None), \
             patch('osm_poi_matchmaker.dataproviders.hu_generic.insert_street_type_dataframe') as mock_insert:
            inst = hu_street_types_from_xml(session=MagicMock(), link='http://example', download_cache='/tmp')
            inst.process()  # must not raise UnboundLocalError

        self.assertFalse(mock_insert.called)


if __name__ == '__main__':
    unittest.main()
