# -*- coding: utf-8 -*-

"""Regression test for libs/pandas.py's save_downloaded_pd() fallback
protection - same class of bug as test_xml.py/test_soup.py: a source serving an
HTML "under maintenance"/error page in place of the real TSV dataset, on an
otherwise-200 response, must fall back to the previously cached file instead of
overwriting it with the bad page.
"""

try:
    import unittest
    import logging
    import os
    import sys
    import shutil
    import tempfile
    from unittest.mock import patch
    from osm_poi_matchmaker.libs.pandas import save_downloaded_pd
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

MAINTENANCE_HTML = ('<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN">'
                     '<html><head><title>Informatikai karbantartas</title></head>'
                     '<body>down</body></html>')

GOOD_TSV = 'postcode\tcity\n1011\tBudapest\n'


class TestSaveDownloadedPdFallback(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp_dir, ignore_errors=True)
        self.cache_path = os.path.join(self.tmp_dir, 'source.tsv')
        patcher1 = patch('osm_poi_matchmaker.libs.pandas.config.get_download_use_cached_data', return_value=False)
        patcher2 = patch('osm_poi_matchmaker.libs.pandas.config.get_directory_cache_url', return_value=self.tmp_dir)
        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)
        patcher1.start()
        patcher2.start()

    def test_good_download_is_cached_and_parsed(self):
        with patch('osm_poi_matchmaker.libs.pandas.download_content', return_value=GOOD_TSV):
            df = save_downloaded_pd('http://example', self.cache_path)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['city'], 'Budapest')

    def test_maintenance_page_falls_back_to_previous_cache_untouched(self):
        with open(self.cache_path, 'w', encoding='utf-8') as f:
            f.write(GOOD_TSV)

        with patch('osm_poi_matchmaker.libs.pandas.download_content', return_value=MAINTENANCE_HTML):
            df = save_downloaded_pd('http://example', self.cache_path)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['city'], 'Budapest')
        with open(self.cache_path, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), GOOD_TSV)  # cache file was never overwritten


if __name__ == '__main__':
    unittest.main()
