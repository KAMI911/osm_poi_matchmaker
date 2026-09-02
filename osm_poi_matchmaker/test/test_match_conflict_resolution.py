# -*- coding: utf-8 -*-

import unittest
import pandas as pd
import numpy as np
from osm_poi_matchmaker.libs.match_conflict_resolution import (
    haversine, find_osm_id_conflicts, resolve_conflict, match_conflict_resolution
)
from osm_poi_matchmaker.libs.string import has_value


class TestHaversine(unittest.TestCase):
    """Test haversine distance calculation."""

    def test_zero_distance(self):
        """Same coordinates should return ~0 meters."""
        dist = haversine(19.0, 47.5, 19.0, 47.5)
        self.assertAlmostEqual(dist, 0, delta=1)

    def test_known_distance(self):
        """Test distance Budapest-Szeged (~170km)."""
        # Budapest: 19.04, 47.50
        # Szeged: 20.14, 46.25
        dist = haversine(19.04, 47.50, 20.14, 46.25)
        # Should be ~170km = 170000 meters
        self.assertGreater(dist, 150000)
        self.assertLess(dist, 190000)

    def test_one_degree_latitude(self):
        """One degree latitude ≈ 111km."""
        dist = haversine(0, 0, 0, 1)
        self.assertGreater(dist, 100000)
        self.assertLess(dist, 115000)


class TestFindConflicts(unittest.TestCase):
    """Test conflict finding."""

    def setUp(self):
        """Create test DataFrame."""
        self.df = pd.DataFrame({
            'poi_id': [1, 2, 3, 4, 5],
            'osm_id': [100, 100, 200, 200, 200],
            'poi_lon': [19.0, 19.01, 19.02, 19.03, 19.04],
            'poi_lat': [47.5, 47.51, 47.52, 47.53, 47.54],
        })

    def test_find_conflicts_basic(self):
        """Should find all osm_id duplicates."""
        conflicts = find_osm_id_conflicts(self.df)
        self.assertEqual(len(conflicts), 2)
        self.assertIn(100, conflicts)
        self.assertIn(200, conflicts)

    def test_find_conflicts_no_duplicates(self):
        """Should return empty dict if no conflicts."""
        df = self.df[self.df['osm_id'] != 100].copy()
        conflicts = find_osm_id_conflicts(df)
        self.assertEqual(len(conflicts), 1)

    def test_find_conflicts_ignores_none(self):
        """Should ignore None osm_id values."""
        self.df.loc[0, 'osm_id'] = None
        conflicts = find_osm_id_conflicts(self.df)
        self.assertNotIn(None, conflicts)


class TestResolveConflict(unittest.TestCase):
    """Test single conflict resolution."""

    def setUp(self):
        """Create test DataFrame with conflicts."""
        self.df = pd.DataFrame({
            'poi_id': [1, 2, 3],
            'osm_id': [100, 100, 200],
            'poi_lon': [19.0, 19.0, 19.02],
            'poi_lat': [47.5, 47.5, 47.52],
            'osm_lon': [19.001, 19.001, 19.02],
            'osm_lat': [47.501, 47.501, 47.52],
        })

    def test_resolve_removes_duplicate(self):
        """Should set osm_id to None for farthest POI."""
        self.df.loc[0, 'osm_lon'] = 19.1
        self.df.loc[0, 'osm_lat'] = 47.6

        initial_conflicts = len(self.df[self.df['osm_id'] == 100])
        self.assertEqual(initial_conflicts, 2)

        resolve_conflict(self.df, 100, [0, 1])

        final_conflicts = len(self.df[self.df['osm_id'] == 100])
        self.assertEqual(final_conflicts, 1)

    def test_resolve_no_change_single(self):
        """Should return False if only one conflict."""
        result = resolve_conflict(self.df, 100, [0])
        self.assertFalse(result)

    def test_resolve_clears_all_osm_derived_fields(self):
        """The demoted (farthest) row must come back to a clean 'never matched'
        state - not just osm_id/osm_node cleared while osm_version/osm_timestamp/
        osm_live_tags/osm_nodes/osm_changeset are left over from the match that was
        just revoked. Left uncleared, file_output.py exports the row as "new" (a
        negative placeholder id, since osm_id is None) while it still carries a
        real OSM version/timestamp/live-tag payload from the element it no longer
        matches - looking like, and partly being, an already-existing element.

        Columns use dtype=object throughout, matching real pipeline data: STAGE 7
        (create_db.py) initializes osm_id/osm_version/etc. to None for every row
        before matching runs, which keeps the column object-dtype even once some
        rows get real int/str values - unlike a plain int column (auto-upcast to
        float64 on the first None assignment, silently turning a later reset into
        NaN instead of None, which 'row.osm_id is None' in file_output.py would
        then miss). A too-narrow test dtype would hide exactly that failure mode.
        """
        self.df['osm_node'] = pd.array(['node', 'node', 'node'], dtype=object)
        self.df['osm_version'] = pd.array(['23', '23', '5'], dtype=object)
        self.df['osm_changeset'] = pd.array([111, 111, 222], dtype=object)
        self.df['osm_timestamp'] = pd.array(
            pd.to_datetime(['2026-06-15', '2026-06-15', '2026-01-01'], utc=True), dtype=object)
        self.df['osm_live_tags'] = pd.array(
            [{'shop': 'supermarket'}, {'shop': 'supermarket'}, {'shop': 'bakery'}], dtype=object)
        self.df['osm_nodes'] = pd.array([None, None, None], dtype=object)
        self.df['osm_id'] = self.df['osm_id'].astype(object)

        # Force row 0 to be the farthest, so it's the one that gets demoted.
        self.df.loc[0, 'osm_lon'] = 19.1
        self.df.loc[0, 'osm_lat'] = 47.6

        resolve_conflict(self.df, 100, [0, 1])

        # Not assertIsNone: pandas silently upcasts a None assignment to whatever
        # missing-value sentinel fits the column's dtype (NaN for numeric, NaT for
        # datetime) - has_value() is what file_output.py actually checks downstream,
        # and it correctly treats every one of those sentinels as "no value".
        demoted = self.df.loc[0]
        for col in ('osm_id', 'osm_node', 'osm_version', 'osm_changeset',
                    'osm_timestamp', 'osm_live_tags', 'osm_nodes'):
            with self.subTest(col=col):
                self.assertFalse(has_value(demoted[col]), f'{col} was not cleared on the demoted row')

        # The winning row (index 1) must be untouched.
        winner = self.df.loc[1]
        self.assertEqual(winner['osm_id'], 100)
        self.assertEqual(winner['osm_version'], '23')
        self.assertEqual(winner['osm_live_tags'], {'shop': 'supermarket'})

    def test_resolve_missing_osm_columns_does_not_crash(self):
        """A DataFrame without osm_version/osm_timestamp/etc. (only the columns the
        rest of this test class already uses) must still resolve cleanly - the
        extra columns are optional, guarded by `if col in data.columns`. Uses
        pd.isna() rather than assertIsNone: this setUp's osm_id is a plain int
        column, so pandas upcasts a None assignment to NaN, not None - the dtype
        nuance itself is covered separately by test_resolve_clears_all_osm_derived_fields."""
        self.df.loc[0, 'osm_lon'] = 19.1
        self.df.loc[0, 'osm_lat'] = 47.6
        result = resolve_conflict(self.df, 100, [0, 1])
        self.assertTrue(result)
        self.assertFalse(has_value(self.df.loc[0, 'osm_id']))


class TestMatchConflictResolution(unittest.TestCase):
    """Test full conflict resolution workflow."""

    def setUp(self):
        """Create test DataFrame with multiple conflicts."""
        self.df = pd.DataFrame({
            'poi_id': [1, 2, 3, 4, 5, 6],
            'osm_id': [100, 100, 200, 200, 300, None],
            'poi_lon': [19.0, 19.05, 19.1, 19.15, 19.2, 19.25],
            'poi_lat': [47.5, 47.5, 47.5, 47.5, 47.5, 47.5],
            'osm_lon': [19.001, 19.001, 19.101, 19.101, 19.201, None],
            'osm_lat': [47.501, 47.501, 47.501, 47.501, 47.501, None],
        })

    def test_resolution_removes_all_conflicts(self):
        """Should resolve all osm_id conflicts."""
        data, stats = match_conflict_resolution(self.df)

        conflicts_before = len(find_osm_id_conflicts(self.df))
        conflicts_after = len(find_osm_id_conflicts(data))

        self.assertGreater(conflicts_before, 0)
        self.assertEqual(conflicts_after, 0)

    def test_resolution_stats(self):
        """Should return correct statistics."""
        data, stats = match_conflict_resolution(self.df)

        self.assertIn('initial_conflicts', stats)
        self.assertIn('resolved', stats)
        self.assertIn('iterations', stats)
        self.assertEqual(stats['unresolved'], 0)

    def test_resolution_max_iterations(self):
        """Should stop at max_iterations."""
        data, stats = match_conflict_resolution(self.df, max_iterations=2)
        self.assertLessEqual(stats['iterations'], 2)

    def test_resolution_large_conflict_group(self):
        """Should handle large conflict groups (5+ POIs same osm_id)."""
        df = pd.DataFrame({
            'osm_id': [100, 100, 100, 100, 100],
            'poi_lon': [19.0, 19.01, 19.02, 19.03, 19.04],
            'poi_lat': [47.5, 47.5, 47.5, 47.5, 47.5],
            'osm_lon': [19.001, 19.001, 19.001, 19.001, 19.001],
            'osm_lat': [47.501, 47.501, 47.501, 47.501, 47.501],
        })
        data, stats = match_conflict_resolution(df)
        self.assertEqual(len(find_osm_id_conflicts(data)), 0)

    def test_resolution_preserves_single_match(self):
        """Should preserve POIs with single osm_id match."""
        df = pd.DataFrame({
            'osm_id': [100, 200, 300],
            'poi_lon': [19.0, 19.1, 19.2],
            'poi_lat': [47.5, 47.5, 47.5],
            'osm_lon': [19.001, 19.101, 19.201],
            'osm_lat': [47.501, 47.501, 47.501],
        })
        data, stats = match_conflict_resolution(df)
        self.assertEqual(len(data), 3)
        self.assertEqual(stats['initial_conflicts'], 0)

    def test_resolution_mixed_conflicts(self):
        """Should handle mix of conflicts and singles."""
        df = pd.DataFrame({
            'osm_id': [100, 100, 200, 300, 300, 300],
            'poi_lon': [19.0, 19.01, 19.1, 19.2, 19.21, 19.22],
            'poi_lat': [47.5, 47.5, 47.5, 47.5, 47.5, 47.5],
            'osm_lon': [19.001, 19.001, 19.101, 19.201, 19.201, 19.201],
            'osm_lat': [47.501, 47.501, 47.501, 47.501, 47.501, 47.501],
        })
        data, stats = match_conflict_resolution(df)
        self.assertEqual(len(find_osm_id_conflicts(data)), 0)
        self.assertEqual(stats['initial_conflicts'], 2)


if __name__ == '__main__':
    unittest.main()
