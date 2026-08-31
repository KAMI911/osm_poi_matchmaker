# -*- coding: utf-8 -*-
"""Test search statistics tracking."""

import unittest
from osm_poi_matchmaker.libs.search_statistics import SearchStatistics, get_search_statistics, reset_search_statistics


class TestSearchStatistics(unittest.TestCase):
    """Test SearchStatistics class."""

    def setUp(self):
        """Create fresh statistics instance for each test."""
        reset_search_statistics()
        self.stats = SearchStatistics()

    def test_record_found_stage_1(self):
        """Should track POI found in stage 1."""
        self.stats.record_found('poi_1', 1)
        self.assertEqual(self.stats.stage_results[1], 1)
        self.assertEqual(self.stats.total_processed, 1)

    def test_record_found_multiple_stages(self):
        """Should track POIs found in different stages."""
        self.stats.record_found('poi_1', 1)
        self.stats.record_found('poi_2', 2)
        self.stats.record_found('poi_3', 3)
        self.stats.record_found('poi_4', 4)

        self.assertEqual(self.stats.stage_results[1], 1)
        self.assertEqual(self.stats.stage_results[2], 1)
        self.assertEqual(self.stats.stage_results[3], 1)
        self.assertEqual(self.stats.stage_results[4], 1)
        self.assertEqual(self.stats.total_processed, 4)

    def test_record_not_found(self):
        """Should track POIs not found."""
        self.stats.record_not_found('poi_missing_1')
        self.stats.record_not_found('poi_missing_2')

        self.assertEqual(len(self.stats.not_found), 2)
        self.assertEqual(self.stats.total_processed, 2)

    def test_mixed_found_and_not_found(self):
        """Should track mix of found and not found."""
        self.stats.record_found('poi_1', 1)
        self.stats.record_found('poi_2', 2)
        self.stats.record_not_found('poi_missing')

        self.assertEqual(self.stats.total_processed, 3)
        self.assertEqual(sum(self.stats.stage_results.values()), 2)
        self.assertEqual(len(self.stats.not_found), 1)

    def test_get_report_empty(self):
        """Should return zero report when no data."""
        report = self.stats.get_report()

        self.assertEqual(report['total'], 0)
        self.assertEqual(report['success_rate'], '0.0%')

    def test_get_report_with_data(self):
        """Should calculate percentages correctly."""
        self.stats.record_found('poi_1', 1)
        self.stats.record_found('poi_2', 1)
        self.stats.record_found('poi_3', 2)
        self.stats.record_not_found('poi_missing')

        report = self.stats.get_report()

        self.assertEqual(report['total'], 4)
        self.assertIn('2', report['stage_1'])  # 2 in stage 1
        self.assertIn('1', report['stage_2'])  # 1 in stage 2
        self.assertIn('75', report['success_rate'])  # 3/4 = 75%

    def test_success_rate_calculation(self):
        """Should calculate success rate correctly."""
        # 100 found, 0 not found = 100%
        for i in range(100):
            self.stats.record_found(f'poi_{i}', 1)

        report = self.stats.get_report()
        self.assertEqual(report['success_rate'], '100.0%')

    def test_success_rate_with_failures(self):
        """Should calculate success rate with failures."""
        # 90 found, 10 not found = 90%
        for i in range(90):
            self.stats.record_found(f'poi_{i}', 1)
        for i in range(10):
            self.stats.record_not_found(f'poi_missing_{i}')

        report = self.stats.get_report()
        self.assertEqual(report['success_rate'], '90.0%')

    def test_found_in_stage_list(self):
        """Should maintain list of POIs per stage."""
        self.stats.record_found('poi_1', 1)
        self.stats.record_found('poi_2', 1)
        self.stats.record_found('poi_3', 2)

        self.assertEqual(len(self.stats.found_in_stage[1]), 2)
        self.assertEqual(len(self.stats.found_in_stage[2]), 1)
        self.assertIn('poi_1', self.stats.found_in_stage[1])
        self.assertIn('poi_3', self.stats.found_in_stage[2])

    def test_export_json(self):
        """Should export as JSON-serializable dict."""
        self.stats.record_found('poi_1', 1)
        self.stats.record_found('poi_2', 2)
        self.stats.record_not_found('poi_missing')

        exported = self.stats.export_json()

        self.assertIn('by_stage', exported)
        self.assertIn('not_found_count', exported)
        self.assertIn('total_processed', exported)
        self.assertIn('success_rate_percent', exported)
        self.assertIn('found_in_stage', exported)

        self.assertEqual(exported['by_stage'][1], 1)
        self.assertEqual(exported['by_stage'][2], 1)
        self.assertEqual(exported['not_found_count'], 1)
        self.assertEqual(exported['total_processed'], 3)

    def test_singleton_pattern(self):
        """Should maintain singleton instance."""
        stats1 = get_search_statistics()
        stats1.record_found('poi_1', 1)

        stats2 = get_search_statistics()
        self.assertEqual(stats2.stage_results[1], 1)
        self.assertIs(stats1, stats2)

    def test_reset_singleton(self):
        """Should reset singleton instance."""
        stats1 = get_search_statistics()
        stats1.record_found('poi_1', 1)

        reset_search_statistics()

        stats2 = get_search_statistics()
        self.assertEqual(stats2.stage_results[1], 0)
        self.assertIsNot(stats1, stats2)


if __name__ == '__main__':
    unittest.main()
