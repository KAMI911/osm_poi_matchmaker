# -*- coding: utf-8 -*-

"""Regression tests for libs/json_cache.py's load_or_fetch_json() - the shared
cache-with-fallback helper for the 7 data providers that fetch JSON directly
via requests (hu_benu.py, hu_ofotert.py, hu_shell.py, hu_rossmann.py,
hu_tesco.py, hu_alzabox.py, hu_dpd.py), replacing their previously-duplicated
inline caching logic. Covers the same class of real-world failure as
test_xml.py/test_soup.py: a source under maintenance can return a
technically-successful but empty result, and that must not silently overwrite
a previously good, non-empty cache.
"""

try:
    import unittest
    import logging
    import os
    import sys
    import json
    import shutil
    import tempfile
    from osm_poi_matchmaker.libs.json_cache import load_or_fetch_json
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestLoadOrFetchJson(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp_dir, ignore_errors=True)
        self.cache_file = os.path.join(self.tmp_dir, 'source.json')

    def test_fresh_non_empty_result_is_cached_and_returned(self):
        result = load_or_fetch_json(self.cache_file, self.tmp_dir, lambda: [{'name': 'A'}], False)

        self.assertEqual(result, [{'name': 'A'}])
        with open(self.cache_file, encoding='utf-8') as f:
            self.assertEqual(json.load(f), [{'name': 'A'}])

    def test_use_cached_data_true_skips_fetch_entirely(self):
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump([{'name': 'cached'}], f)

        called = []
        result = load_or_fetch_json(self.cache_file, self.tmp_dir, lambda: called.append(1) or [], True)

        self.assertEqual(result, [{'name': 'cached'}])
        self.assertEqual(called, [])

    def test_empty_fetch_result_falls_back_to_previous_non_empty_cache(self):
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump([{'name': 'good'}], f)

        result = load_or_fetch_json(self.cache_file, self.tmp_dir, lambda: [], False)

        self.assertEqual(result, [{'name': 'good'}])
        with open(self.cache_file, encoding='utf-8') as f:
            self.assertEqual(json.load(f), [{'name': 'good'}])  # cache was never overwritten with []

    def test_raising_fetch_falls_back_to_previous_cache(self):
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump([{'name': 'good'}], f)

        def boom():
            raise Exception('source down')

        result = load_or_fetch_json(self.cache_file, self.tmp_dir, boom, False)

        self.assertEqual(result, [{'name': 'good'}])

    def test_raising_fetch_with_no_previous_cache_returns_none_without_raising(self):
        def boom():
            raise Exception('source down')

        result = load_or_fetch_json(self.cache_file, self.tmp_dir, boom, False)

        self.assertIsNone(result)
        self.assertFalse(os.path.isfile(self.cache_file))

    def test_empty_fetch_with_no_previous_cache_writes_the_empty_result(self):
        # No previous data to protect - an empty result here is presumably genuine
        # (e.g. a brand-new provider with truly zero locations) rather than a
        # maintenance-page symptom, so it's written and returned as-is.
        result = load_or_fetch_json(self.cache_file, self.tmp_dir, lambda: [], False)

        self.assertEqual(result, [])
        self.assertTrue(os.path.isfile(self.cache_file))

    def test_creates_download_cache_directory_if_missing(self):
        nested_dir = os.path.join(self.tmp_dir, 'nested')
        nested_cache_file = os.path.join(nested_dir, 'source.json')

        result = load_or_fetch_json(nested_cache_file, nested_dir, lambda: [{'name': 'A'}], False)

        self.assertEqual(result, [{'name': 'A'}])
        self.assertTrue(os.path.isdir(nested_dir))


if __name__ == '__main__':
    unittest.main()
