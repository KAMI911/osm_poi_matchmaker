# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    from osm_poi_matchmaker.dao.data_handlers import _safe_str
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestSafeStr(unittest.TestCase):
    """Regression tests for _safe_str(), used by insert_patch_data_dataframe() to
    build POI_patch rows from poi_patch.tsv. A bare str(row.x) turned a None cell
    (any optional patch field - orig_conscriptionnumber is None for ~99% of real
    rows) into the literal 4-character text 'None' instead of SQL NULL - live in
    the poi_patch table before this fix.
    """

    def test_none_stays_none(self):
        self.assertIsNone(_safe_str(None))

    def test_nan_becomes_none(self):
        self.assertIsNone(_safe_str(float('nan')))

    def test_real_string_is_kept(self):
        self.assertEqual(_safe_str('Kossuth utca'), 'Kossuth utca')

    def test_real_number_is_stringified(self):
        self.assertEqual(_safe_str(1026), '1026')

    def test_result_is_never_the_literal_text_none(self):
        self.assertNotEqual(_safe_str(None), 'None')
