# -*- coding: utf-8 -*-

"""Regression tests for libs/soup.py's save_downloaded_soup() fallback
protection - the save_downloaded_soup() counterpart to test_xml.py. Used by 66
of the project's ~67 active data providers, so this is the single highest-
leverage place to guard against a source silently serving an HTML "under
maintenance"/error page (or, for JSON, a syntactically broken body) in place of
the real feed on an otherwise-200 response - the same class of bug that used to
crash the pipeline via hu_generic.py's XML feeds. Such a response must be
treated the same as a failed download: fall back to whatever was already
cached from a previous successful run, and never overwrite that cache file
with the bad response.
"""

try:
    import unittest
    import logging
    import os
    import sys
    import shutil
    import tempfile
    from unittest.mock import patch
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup, _is_usable_content
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

MAINTENANCE_HTML = ('<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN">'
                     '<html><head><title>Informatikai karbantartas</title></head>'
                     '<body>down</body></html>')

GOOD_JSON = '{"stores": [{"name": "Test store"}]}'
GOOD_XML = '<?xml version="1.0" encoding="UTF-8"?><root><item>1</item></root>'
GOOD_CSV = 'postcode\tcity\n1011\tBudapest\n'


class TestIsUsableContent(unittest.TestCase):

    def test_html_maintenance_page_rejected_for_json(self):
        self.assertFalse(_is_usable_content(MAINTENANCE_HTML, FileType.json, 'http://example'))

    def test_html_maintenance_page_rejected_for_xml(self):
        self.assertFalse(_is_usable_content(MAINTENANCE_HTML, FileType.xml, 'http://example'))

    def test_html_maintenance_page_rejected_for_csv(self):
        self.assertFalse(_is_usable_content(MAINTENANCE_HTML, FileType.csv, 'http://example'))

    def test_malformed_json_rejected(self):
        self.assertFalse(_is_usable_content('{"stores": [', FileType.json, 'http://example'))

    def test_good_json_accepted(self):
        self.assertTrue(_is_usable_content(GOOD_JSON, FileType.json, 'http://example'))

    def test_good_xml_accepted(self):
        self.assertTrue(_is_usable_content(GOOD_XML, FileType.xml, 'http://example'))

    def test_good_csv_accepted(self):
        self.assertTrue(_is_usable_content(GOOD_CSV, FileType.csv, 'http://example'))


class TestSaveDownloadedSoupFallback(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp_dir, ignore_errors=True)
        self.cache_path = os.path.join(self.tmp_dir, 'source.json')
        patcher1 = patch('osm_poi_matchmaker.libs.soup.config.get_download_use_cached_data', return_value=False)
        patcher2 = patch('osm_poi_matchmaker.libs.soup.config.get_directory_cache_url', return_value=self.tmp_dir)
        patcher3 = patch('osm_poi_matchmaker.libs.soup.is_downloaded', return_value=False)
        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)
        self.addCleanup(patcher3.stop)
        patcher1.start()
        patcher2.start()
        patcher3.start()

    def test_good_json_is_cached_and_returned(self):
        with patch('osm_poi_matchmaker.libs.soup.download_content', return_value=GOOD_JSON):
            result = save_downloaded_soup('http://example', self.cache_path, FileType.json)

        self.assertEqual(result, GOOD_JSON)
        with open(self.cache_path, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), GOOD_JSON)

    def test_maintenance_page_falls_back_to_previous_json_cache_untouched(self):
        with open(self.cache_path, 'w', encoding='utf-8') as f:
            f.write(GOOD_JSON)

        with patch('osm_poi_matchmaker.libs.soup.download_content', return_value=MAINTENANCE_HTML):
            result = save_downloaded_soup('http://example', self.cache_path, FileType.json)

        self.assertEqual(result, GOOD_JSON)
        with open(self.cache_path, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), GOOD_JSON)  # cache file was never overwritten

    def test_malformed_json_falls_back_to_previous_cache_untouched(self):
        with open(self.cache_path, 'w', encoding='utf-8') as f:
            f.write(GOOD_JSON)

        with patch('osm_poi_matchmaker.libs.soup.download_content', return_value='{"stores": ['):
            result = save_downloaded_soup('http://example', self.cache_path, FileType.json)

        self.assertEqual(result, GOOD_JSON)
        with open(self.cache_path, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), GOOD_JSON)

    def test_bad_download_with_no_previous_cache_returns_none_without_raising(self):
        with patch('osm_poi_matchmaker.libs.soup.download_content', return_value=MAINTENANCE_HTML):
            result = save_downloaded_soup('http://example', self.cache_path, FileType.json)

        self.assertIsNone(result)
        self.assertFalse(os.path.isfile(self.cache_path))

    def test_maintenance_page_falls_back_to_previous_xml_cache_untouched(self):
        xml_cache_path = os.path.join(self.tmp_dir, 'source.xml')
        with open(xml_cache_path, 'w', encoding='utf-8') as f:
            f.write(GOOD_XML)

        with patch('osm_poi_matchmaker.libs.soup.download_content', return_value=MAINTENANCE_HTML):
            save_downloaded_soup('http://example', xml_cache_path, FileType.xml)

        with open(xml_cache_path, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), GOOD_XML)  # cache file was never overwritten

    def test_real_html_scrape_is_not_affected_by_the_guard(self):
        # FileType.html sources are genuinely expected to be HTML - the guard
        # must not reject their own real content.
        html_cache_path = os.path.join(self.tmp_dir, 'source.html')
        with patch('osm_poi_matchmaker.libs.soup.download_content', return_value=MAINTENANCE_HTML):
            result = save_downloaded_soup('http://example', html_cache_path, FileType.html)

        self.assertIsNotNone(result)
        self.assertTrue(os.path.isfile(html_cache_path))


if __name__ == '__main__':
    unittest.main()
