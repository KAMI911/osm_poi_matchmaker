# -*- coding: utf-8 -*-

import unittest
import pandas as pd
import numpy as np
from osm_poi_matchmaker.libs.match_conflict_resolution import (
    haversine, find_osm_id_conflicts, resolve_conflict, match_conflict_resolution
)


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


if __name__ == '__main__':
    unittest.main()
