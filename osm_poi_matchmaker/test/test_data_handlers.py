# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    from datetime import datetime, timedelta, timezone
    from unittest.mock import MagicMock, patch
    from osm_poi_matchmaker.dao.data_handlers import _safe_str, get_or_create_cache
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


class _FakeCacheRow:
    """Stand-in for POI_OSM_cache: a plain constructor-by-kwargs object, since
    these tests exercise get_or_create_cache()'s own logic, not the ORM/DB."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestGetOrCreateCache(unittest.TestCase):
    """poi_osm_cache is explicitly meant to survive across runs (see
    POI_OSM_cache's docstring), so a row found by (osm_id, osm_object_type) must
    not be trusted forever: get_or_create_cache() has to refresh a stale one in
    place instead of just handing back the old data - otherwise
    query_from_cache()'s staleness check (poi_base.py) is pointless, since a
    fresh re-fetch triggered by its miss would loop right back into here, find
    the same old row again, and get the same stale answer back."""

    def setUp(self):
        self.session = MagicMock()
        self.query_mock = self.session.query.return_value.filter_by.return_value
        self.max_age_patcher = patch(
            'osm_poi_matchmaker.dao.data_handlers.config.get_database_osm_cache_max_age_days',
            return_value=7)
        self.max_age_patcher.start()
        self.addCleanup(self.max_age_patcher.stop)

    def test_creates_new_instance_when_none_exists(self):
        self.query_mock.first.return_value = None
        result = get_or_create_cache(self.session, _FakeCacheRow, osm_id=100, osm_object_type='node',
                                     osm_version=5)
        self.assertEqual(result.osm_id, 100)
        self.assertEqual(result.osm_version, 5)
        self.session.add.assert_called_once()
        self.session.commit.assert_called_once()

    def test_fresh_existing_instance_is_returned_without_writing(self):
        existing = _FakeCacheRow(osm_id=100, osm_object_type='node', osm_version=5,
                                 poc_fetched_at=datetime.now(timezone.utc) - timedelta(days=1))
        self.query_mock.first.return_value = existing
        result = get_or_create_cache(self.session, _FakeCacheRow, osm_id=100, osm_object_type='node',
                                     osm_version=6)
        self.assertIs(result, existing)
        self.assertEqual(result.osm_version, 5, 'a fresh row must not be overwritten')
        self.session.commit.assert_not_called()

    def test_stale_existing_instance_is_refreshed_in_place(self):
        old_fetched_at = datetime.now(timezone.utc) - timedelta(days=30)
        existing = _FakeCacheRow(osm_id=100, osm_object_type='node', osm_version=5,
                                 poc_fetched_at=old_fetched_at)
        self.query_mock.first.return_value = existing
        result = get_or_create_cache(self.session, _FakeCacheRow, osm_id=100, osm_object_type='node',
                                     osm_version=6)
        self.assertIs(result, existing, 'must refresh in place, not silently create a second row')
        self.assertEqual(result.osm_version, 6, 'stale row was not refreshed with the new data')
        self.assertNotEqual(result.poc_fetched_at, old_fetched_at, 'poc_fetched_at was not bumped')
        self.session.commit.assert_called_once()
        self.session.add.assert_not_called()

    def test_instance_without_poc_fetched_at_is_never_treated_as_stale(self):
        """Defensive: get_or_create_cache() is written generically (takes `model`
        as a parameter), even though poi_osm_cache is its only real caller today -
        a model with no poc_fetched_at column at all must keep the original
        'always return the existing row' behaviour, not crash or misbehave."""
        existing = _FakeCacheRow(osm_id=100, osm_object_type='node', osm_version=5)
        self.query_mock.first.return_value = existing
        result = get_or_create_cache(self.session, _FakeCacheRow, osm_id=100, osm_object_type='node',
                                     osm_version=6)
        self.assertIs(result, existing)
        self.assertEqual(result.osm_version, 5)
        self.session.commit.assert_not_called()

    def test_missing_required_kwargs_returns_none(self):
        result = get_or_create_cache(self.session, _FakeCacheRow, osm_id=None, osm_object_type='node')
        self.assertIsNone(result)
        self.session.query.assert_not_called()
