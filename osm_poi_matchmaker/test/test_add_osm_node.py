# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    import datetime
    import pandas as pd
    from osm_poi_matchmaker.libs.file_output import add_osm_node, add_osm_way, list_osm_node
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestAddOsmNodeTimestampFallback(unittest.TestCase):
    """Regression tests for add_osm_node()/add_osm_way()/list_osm_node()'s
    osm_timestamp/osm_version fallback.

    'x is None' doesn't catch pandas.NaT, which match_conflict_resolution.py's
    demoted-row reset can leave a datetime-dtype osm_timestamp column with instead
    of None (pandas upcasts a None assignment to whatever missing-value sentinel
    fits the column's dtype). TIMESTAMP_FORMAT.format() on a NaT raises
    "NaTType does not support strftime" instead of falling back to datetime.now()
    - this used to crash export for exactly the kind of row conflict resolution
    demotes.
    """

    def _node_data(self, **overrides):
        base = {'poi_lat': 47.5, 'poi_lon': 19.0, 'osm_timestamp': None, 'osm_version': None}
        base.update(overrides)
        return base

    def test_nat_timestamp_does_not_crash_add_osm_node(self):
        node_data = self._node_data(osm_timestamp=pd.NaT)
        result = add_osm_node(-1, node_data)
        self.assertIn('timestamp', result)

    def test_nat_timestamp_does_not_crash_add_osm_way(self):
        node_data = self._node_data(osm_timestamp=pd.NaT)
        result = add_osm_way(-1, node_data)
        self.assertIn('timestamp', result)

    def test_nat_timestamp_does_not_crash_list_osm_node(self):
        node_data = self._node_data(osm_timestamp=pd.NaT)
        result = list_osm_node(123456, node_data)
        self.assertIn('timestamp', result)

    def test_nan_version_falls_back_to_default(self):
        node_data = self._node_data(osm_version=float('nan'))
        result = add_osm_node(-1, node_data)
        self.assertEqual(result['version'], '99999')

    def test_none_timestamp_falls_back_to_now(self):
        node_data = self._node_data(osm_timestamp=None)
        result = add_osm_node(-1, node_data)
        self.assertIn('timestamp', result)

    def test_real_timestamp_and_version_are_kept(self):
        ts = datetime.datetime(2026, 6, 15, 9, 59, 15)
        node_data = self._node_data(osm_timestamp=ts, osm_version='23')
        result = add_osm_node(123456, node_data)
        self.assertEqual(result['version'], '23')
        self.assertIn('2026-06-15', result['timestamp'])

    def test_action_is_always_modify(self):
        """JOSM's .osm file format only documents 'modify'/'delete' as action
        values - a new element (negative id) is signalled by the id alone, not by
        an action='create' (not a real JOSM value)."""
        self.assertEqual(add_osm_node(-1, self._node_data())['action'], 'modify')
        self.assertEqual(add_osm_node(123456, self._node_data())['action'], 'modify')
