# -*- coding: utf-8 -*-

"""Regression tests for libs/osm.py's query_postcode_osm_external().

Real-world bug: with the (then-default) geo.prefer.osm.postcode=True, OSM's own
boundary=postal_code polygon lookup always won over the data provider's own
postcode whenever it had *any* candidate - even an imprecise one. Confirmed live:
Rossmann/Raiffeisen at Hunyadi János út 19, Budapest (postcode 1117) got tagged
1116 by this project's own OSM account, because OSM's community-drawn "1116
Budapest XI. kerület" boundary polygon imprecisely contains that address point,
and the function never even looked at the provider's own (correct) postcode.

Separately, flipping prefer_osm to False to fix that exposed a second, genuine
bug: with prefer_osm=False, if the provider had no postcode at all, the function
queried the boundary polygon anyway but then discarded that result and returned
None, instead of using it as a fallback - so a provider that didn't supply a
postcode would end up with no postcode at all, where it previously (with
prefer_osm=True) at least got the polygon-derived guess.

This file covers both: the preferred source always wins when it has a candidate,
and the *other* source is always used as a fallback instead of being silently
dropped.
"""

try:
    import unittest
    import logging
    import sys
    from unittest.mock import MagicMock, patch
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestQueryPostcodeOsmExternal(unittest.TestCase):

    def setUp(self):
        self.session = MagicMock()

    def _with_boundary(self, value):
        return patch('osm_poi_matchmaker.libs.osm.query_osm_postcode_gpd', return_value=value)

    def test_existing_osm_postcode_always_wins_when_prefer_original(self):
        with self._with_boundary(1116) as mock_q:
            result = query_postcode_osm_external(True, True, self.session, 19.04, 47.43, '1117', '1120')
        self.assertEqual(result, '1120')
        mock_q.assert_not_called()

    def test_prefer_osm_false_with_provider_postcode_skips_boundary_query(self):
        """The real-world bug: with prefer_osm=False and a provider postcode
        present, the boundary polygon must not even be queried, let alone
        override it."""
        with self._with_boundary(1116) as mock_q:
            result = query_postcode_osm_external(False, True, self.session, 19.04, 47.43, '1117', None)
        self.assertEqual(result, '1117')
        mock_q.assert_not_called()

    def test_prefer_osm_false_with_no_provider_postcode_falls_back_to_boundary(self):
        """The second bug: prefer_osm=False must not silently drop an already-
        queried boundary-polygon result just because it wasn't the preferred
        source - it's the only candidate available here."""
        with self._with_boundary(1116):
            result = query_postcode_osm_external(False, True, self.session, 19.04, 47.43, None, None)
        self.assertEqual(result, '1116')

    def test_prefer_osm_true_with_boundary_match_wins_over_provider(self):
        with self._with_boundary(1116):
            result = query_postcode_osm_external(True, True, self.session, 19.04, 47.43, '1117', None)
        self.assertEqual(result, '1116')

    def test_prefer_osm_true_falls_back_to_provider_when_boundary_empty(self):
        with self._with_boundary(None):
            result = query_postcode_osm_external(True, True, self.session, 19.04, 47.43, '1117', None)
        self.assertEqual(result, '1117')

    def test_nothing_usable_returns_none(self):
        with self._with_boundary(None):
            result = query_postcode_osm_external(True, True, self.session, 19.04, 47.43, None, None)
        self.assertIsNone(result)

    def test_invalid_provider_postcode_is_not_used(self):
        """clean_postcode() rejects non-4-digit values - an invalid provider
        postcode must fall back to the boundary polygon, not pass through as-is."""
        with self._with_boundary(1116):
            result = query_postcode_osm_external(False, True, self.session, 19.04, 47.43, '10003 - Mobiliti', None)
        self.assertEqual(result, '1116')


if __name__ == '__main__':
    unittest.main()
