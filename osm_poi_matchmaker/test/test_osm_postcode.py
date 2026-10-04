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

Also covers a separate, third bug in the same area: query_osm_postcode_gpd()'s
SQL picked among several overlapping boundary=postal_code polygons with `ORDER
BY name LIMIT 1` - an alphabetical, geometrically-meaningless tie-break. Hand-
drawn OSM postal boundaries do overlap in practice (confirmed live: Budapest's
1044/1047 IV. kerület polygons do), and this is how a real Hatvan pharmacy
(postcode 3000, a provider that never supplies its own postcode at all) once
got tagged 2173 - a neighbouring town's polygon apparently also covered that
point, and '2173' sorts before '3000'. Fixed to prefer the smallest (most
specific) containing polygon instead, via `ORDER BY ST_Area(way)`.
"""

try:
    import unittest
    import logging
    import sys
    from unittest.mock import MagicMock, patch
    from osm_poi_matchmaker.libs.osm import query_postcode_osm_external, query_osm_postcode_gpd
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


class TestQueryOsmPostcodeGpd(unittest.TestCase):
    """query_osm_postcode_gpd() must break ties between overlapping
    boundary=postal_code polygons by picking the smallest (most specific) one,
    not whichever sorts first alphabetically by name - see the module docstring."""

    def test_orders_by_area_not_by_name(self):
        session = MagicMock()
        session.execute.return_value.first.return_value = None
        query_osm_postcode_gpd(session, 19.08, 47.57)

        sql_text = str(session.execute.call_args[0][0])
        self.assertIn('ST_Area', sql_text)
        self.assertNotIn('ORDER BY name', sql_text)

    def test_returns_the_smaller_overlapping_polygons_postcode(self):
        """Regression case for the real Hatvan/Kartal mix-up: two overlapping
        polygons both contain the point - the smaller one's postcode must win."""
        session = MagicMock()
        # ORDER BY ST_Area(way) LIMIT 1 means the DB itself only ever returns the
        # winning (smallest) row - simulate that DB-side selection directly.
        session.execute.return_value.first.return_value = MagicMock(
            _mapping={'name': '1047 Budapest IV. kerület'})
        result = query_osm_postcode_gpd(session, 19.08, 47.57)
        self.assertEqual(result, 1047)


if __name__ == '__main__':
    unittest.main()
