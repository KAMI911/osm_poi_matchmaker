# -*- coding: utf-8 -*-

import unittest
import pandas as pd
import numpy as np
from datetime import datetime
from osm_poi_matchmaker.libs.quality_metrics import (
    QualityMetrics, calculate_quality_metrics
)


class TestQualityMetricsInit(unittest.TestCase):
    """Test QualityMetrics initialization."""

    def test_init(self):
        """Should initialize with data and stage name."""
        df = pd.DataFrame({'col': [1, 2, 3]})
        metrics = QualityMetrics(df, 'TEST_STAGE')
        self.assertEqual(metrics.stage_name, 'TEST_STAGE')
        self.assertEqual(len(metrics.data), 3)
        self.assertIsNotNone(metrics.timestamp)

    def test_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame()
        metrics = QualityMetrics(df, 'EMPTY')
        self.assertEqual(len(metrics.data), 0)


class TestQualityMetricsCalculate(unittest.TestCase):
    """Test metrics calculation."""

    def setUp(self):
        """Create test DataFrame."""
        self.df = pd.DataFrame({
            'poi_lon': [19.0, 19.1, None, 19.3],
            'poi_lat': [47.5, 47.6, 47.7, None],
            'poi_postcode': ['1011', None, '1013', '1015'],
            'poi_name': ['POI1', 'POI2', None, 'POI4'],
            'poi_city': ['Budapest', 'Debrecen', 'Szeged', None],
            'osm_id': [100, 100, 200, None],
            'poi_new': [False, True, False, False],
            'match_error': [False, False, True, False],
            'poi_addr_street': ['Str1', 'Str2', 'Str3', 'Str4'],
            'poi_addr_housenumber': ['1', '2', '3', '4'],
        })

    def test_calculate_metrics(self):
        """Should calculate all metrics."""
        metrics = QualityMetrics(self.df, 'STAGE_X')
        result = metrics.calculate()

        self.assertIn('timestamp', result)
        self.assertIn('stage', result)
        self.assertIn('total_records', result)
        self.assertIn('valid_records', result)
        self.assertIn('error_rate_percent', result)
        self.assertIn('completion_rate_percent', result)

    def test_total_records(self):
        """Should count total records."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        self.assertEqual(result['total_records'], 4)

    def test_valid_records(self):
        """Should count records with essential fields."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        # Need both lat/lon AND (name OR city)
        # Row 0: ✓ lon, ✓ lat, ✓ name
        # Row 1: ✓ lon, ✓ lat, ✓ name
        # Row 2: None lon, ✓ lat, None name (has city) → valid
        # Row 3: ✓ lon, None lat, ✓ name → invalid
        self.assertGreater(result['valid_records'], 0)

    def test_missing_coordinates(self):
        """Should count records with missing lat/lon."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        # Row 2: None lon; Row 3: None lat
        self.assertEqual(result['missing_coordinates'], 2)

    def test_missing_postcode(self):
        """Should count records with missing postcode."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        self.assertEqual(result['missing_postcode'], 1)

    def test_missing_name(self):
        """Should count records with missing name."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        self.assertEqual(result['missing_name'], 1)

    def test_duplicates(self):
        """Should count OSM ID duplicates."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        # osm_id: [100, 100, 200, None] → 1 duplicate group (100)
        self.assertGreaterEqual(result['duplicates'], 1)

    def test_osm_matched(self):
        """Should count records with osm_id."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        self.assertEqual(result['osm_matched'], 3)

    def test_osm_new(self):
        """Should count new POIs."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        self.assertEqual(result['osm_new'], 1)

    def test_errors(self):
        """Should count records with match_error."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        self.assertEqual(result['errors'], 1)

    def test_error_rate_percent(self):
        """Should calculate error rate percentage."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        expected = (1 / 4) * 100
        self.assertAlmostEqual(result['error_rate_percent'], expected, places=1)

    def test_completion_rate_percent(self):
        """Should calculate completion rate percentage."""
        metrics = QualityMetrics(self.df, 'TEST')
        result = metrics.calculate()
        self.assertGreater(result['completion_rate_percent'], 0)
        self.assertLessEqual(result['completion_rate_percent'], 100)

    def test_zero_division_protection(self):
        """Should handle empty DataFrame gracefully."""
        df = pd.DataFrame({
            'poi_lon': [],
            'poi_lat': [],
            'osm_id': [],
        })
        metrics = QualityMetrics(df, 'EMPTY')
        result = metrics.calculate()
        self.assertEqual(result['error_rate_percent'], 0)


class TestQualityMetricsGetSummary(unittest.TestCase):
    """Test summary generation."""

    def setUp(self):
        """Create test DataFrame."""
        self.df = pd.DataFrame({
            'poi_lon': [19.0, 19.1, 19.2],
            'poi_lat': [47.5, 47.6, 47.7],
            'poi_postcode': ['1011', '1013', '1015'],
            'poi_name': ['POI1', 'POI2', 'POI3'],
            'poi_city': ['Budapest', 'Debrecen', 'Szeged'],
            'osm_id': [100, 200, 300],
            'poi_new': [False, False, False],
            'match_error': [False, False, False],
            'poi_addr_street': ['Str1', 'Str2', 'Str3'],
            'poi_addr_housenumber': ['1', '2', '3'],
        })

    def test_get_summary(self):
        """Should return formatted summary."""
        metrics = QualityMetrics(self.df, 'STAGE_X')
        summary = metrics.get_summary()

        self.assertIn('stage', summary)
        self.assertIn('total', summary)
        self.assertIn('valid', summary)
        self.assertIn('osm_matched', summary)
        self.assertIn('errors', summary)

    def test_summary_format_has_percentages(self):
        """Should include percentages in summary strings."""
        metrics = QualityMetrics(self.df, 'STAGE_X')
        summary = metrics.get_summary()

        self.assertIn('%', summary['valid'])
        self.assertIn('%', summary['osm_matched'])


class TestQualityMetricsExport(unittest.TestCase):
    """Test metrics export."""

    def setUp(self):
        """Create test DataFrame."""
        self.df = pd.DataFrame({
            'poi_lon': [19.0, 19.1],
            'poi_lat': [47.5, 47.6],
            'poi_postcode': ['1011', '1013'],
            'poi_name': ['POI1', 'POI2'],
            'poi_city': ['Budapest', 'Debrecen'],
            'osm_id': [100, 200],
        })

    def test_export_json(self):
        """Should export metrics as JSON dict."""
        metrics = QualityMetrics(self.df, 'EXPORT_TEST')
        exported = metrics.export_json()

        self.assertIsInstance(exported, dict)
        self.assertIn('timestamp', exported)
        self.assertIn('total_records', exported)

    def test_export_without_calculate(self):
        """Should auto-calculate on export."""
        metrics = QualityMetrics(self.df, 'AUTO_CALC')
        exported = metrics.export_json()

        self.assertGreater(exported['total_records'], 0)

    def test_log_summary(self):
        """Should log summary without error."""
        metrics = QualityMetrics(self.df, 'LOG_TEST')
        # Should not raise exception
        metrics.log_summary()


class TestCalculateQualityMetricsFunction(unittest.TestCase):
    """Test convenience function."""

    def test_calculate_quality_metrics_valid(self):
        """Should calculate and log metrics."""
        df = pd.DataFrame({
            'poi_lon': [19.0, 19.1],
            'poi_lat': [47.5, 47.6],
            'poi_postcode': ['1011', '1013'],
            'poi_name': ['POI1', 'POI2'],
            'poi_city': ['Budapest', 'Debrecen'],
            'osm_id': [100, 200],
        })
        metrics = calculate_quality_metrics(df, 'TEST_STAGE')

        self.assertIsInstance(metrics, dict)
        self.assertEqual(metrics['stage'], 'TEST_STAGE')
        self.assertEqual(metrics['total_records'], 2)

    def test_calculate_quality_metrics_empty_stage(self):
        """Should handle empty stage name."""
        df = pd.DataFrame({
            'poi_lon': [19.0],
            'poi_lat': [47.5],
        })
        metrics = calculate_quality_metrics(df, '')
        self.assertEqual(metrics['stage'], '')


class TestQualityMetricsWithMissingColumns(unittest.TestCase):
    """Test robustness with missing DataFrame columns."""

    def test_missing_poi_new_column(self):
        """Should handle missing poi_new column."""
        df = pd.DataFrame({
            'poi_lon': [19.0],
            'poi_lat': [47.5],
            'osm_id': [100],
        })
        metrics = QualityMetrics(df, 'TEST')
        result = metrics.calculate()
        self.assertEqual(result['osm_new'], 0)

    def test_missing_match_error_column(self):
        """Should handle missing match_error column."""
        df = pd.DataFrame({
            'poi_lon': [19.0],
            'poi_lat': [47.5],
            'osm_id': [100],
        })
        metrics = QualityMetrics(df, 'TEST')
        result = metrics.calculate()
        self.assertEqual(result['errors'], 0)


if __name__ == '__main__':
    unittest.main()
