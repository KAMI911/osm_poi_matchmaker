# -*- coding: utf-8 -*-

"""Regression tests for libs/xml.py's save_downloaded_xml() fallback protection:
a source occasionally serves an HTML "under maintenance"/error page in place of
the real XML on an otherwise-200 response (seen live with Magyar Posta's
ZipCodes.xml/StreetTypes.xml). Such a response - and any other malformed
content - must be treated the same as a failed download: fall back to reading
whatever was already cached from a previous successful run, and never
overwrite that cache file with the bad page.
"""

try:
    import unittest
    import logging
    import os
    import sys
    import shutil
    import tempfile
    from unittest.mock import patch
    from osm_poi_matchmaker.libs.xml import save_downloaded_xml, _is_usable_xml
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

MAINTENANCE_HTML = (b'<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN">'
                     b'<html><head><title>Informatikai karbantartas</title></head>'
                     b'<body>down</body></html>')

TRUNCATED_GARBAGE = b'<zipCodes><zipCode><postCode>1011</postCod'

GOOD_XML = (b'<?xml version="1.0" encoding="UTF-8"?>'
             b'<zipCodes><zipCode><postCode>1011</postCode><cities>Budapest</cities></zipCode></zipCodes>')


class TestIsUsableXml(unittest.TestCase):

    def test_html_maintenance_page_rejected(self):
        self.assertFalse(_is_usable_xml(MAINTENANCE_HTML, 'http://example'))

    def test_truncated_garbage_rejected(self):
        self.assertFalse(_is_usable_xml(TRUNCATED_GARBAGE, 'http://example'))

    def test_good_xml_accepted(self):
        self.assertTrue(_is_usable_xml(GOOD_XML, 'http://example'))


class TestSaveDownloadedXmlFallback(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp_dir, ignore_errors=True)
        self.cache_path = os.path.join(self.tmp_dir, 'hu_city_postcode.xml')

    def test_good_download_is_cached_and_returned(self):
        with patch('osm_poi_matchmaker.libs.xml.download_xml', return_value=GOOD_XML):
            result = save_downloaded_xml('http://example', self.cache_path)

        self.assertEqual(result, GOOD_XML)
        with open(self.cache_path, 'rb') as f:
            self.assertEqual(f.read(), GOOD_XML)

    def test_maintenance_page_falls_back_to_previous_cache_untouched(self):
        with open(self.cache_path, 'wb') as f:
            f.write(GOOD_XML)

        with patch('osm_poi_matchmaker.libs.xml.download_xml', return_value=MAINTENANCE_HTML):
            result = save_downloaded_xml('http://example', self.cache_path)

        self.assertEqual(result, GOOD_XML)
        with open(self.cache_path, 'rb') as f:
            self.assertEqual(f.read(), GOOD_XML)  # cache file was never overwritten

    def test_truncated_garbage_falls_back_to_previous_cache_untouched(self):
        with open(self.cache_path, 'wb') as f:
            f.write(GOOD_XML)

        with patch('osm_poi_matchmaker.libs.xml.download_xml', return_value=TRUNCATED_GARBAGE):
            result = save_downloaded_xml('http://example', self.cache_path)

        self.assertEqual(result, GOOD_XML)
        with open(self.cache_path, 'rb') as f:
            self.assertEqual(f.read(), GOOD_XML)

    def test_connection_failure_falls_back_to_previous_cache(self):
        with open(self.cache_path, 'wb') as f:
            f.write(GOOD_XML)

        with patch('osm_poi_matchmaker.libs.xml.download_xml', return_value=None):
            result = save_downloaded_xml('http://example', self.cache_path)

        self.assertEqual(result, GOOD_XML)

    def test_bad_download_with_no_previous_cache_returns_none_without_raising(self):
        with patch('osm_poi_matchmaker.libs.xml.download_xml', return_value=MAINTENANCE_HTML):
            result = save_downloaded_xml('http://example', self.cache_path)

        self.assertIsNone(result)
        self.assertFalse(os.path.isfile(self.cache_path))


if __name__ == '__main__':
    unittest.main()
