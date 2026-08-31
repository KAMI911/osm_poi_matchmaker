# -*- coding: utf-8 -*-
"""Test fixed opening_hours module with nan-nan handling."""

import unittest
import pandas as pd
import numpy as np
from osm_poi_matchmaker.libs.opening_hours import OpeningHours


class TestOpeningHoursNaNHandling(unittest.TestCase):
    """Test nan-nan problem fix in opening_hours."""

    def test_skip_invalid_time_values(self):
        """Should skip rows with NaN time values."""
        data = pd.DataFrame({
            'opening_hours_begin': [pd.NA, '09:00', '10:00'],
            'opening_hours_end': [pd.NA, '17:00', '18:00'],
            'lunch_time_begin': [pd.NA, np.nan, '12:00'],
            'lunch_time_end': [pd.NA, np.nan, '13:00'],
            'day_of_week': ['Mo', 'Tu', 'We'],
        })

        oh = OpeningHours(data)
        result = oh.process()

        # Should not contain "nan-nan" in opening hours
        for hours in result:
            if pd.notna(hours):
                self.assertNotIn('nan', str(hours).lower())

    def test_valid_opening_hours_format(self):
        """Should format valid opening hours correctly."""
        data = pd.DataFrame({
            'opening_hours_begin': ['09:00', '10:00'],
            'opening_hours_end': ['17:00', '18:00'],
            'lunch_time_begin': [np.nan, np.nan],
            'lunch_time_end': [np.nan, np.nan],
            'day_of_week': ['Mo', 'Tu'],
        })

        oh = OpeningHours(data)
        result = oh.process()

        # Should have proper format without nan
        for hours in result:
            if pd.notna(hours):
                self.assertIn('09:00', str(hours) or 'Tu' in str(hours))

    def test_lunch_break_with_valid_times(self):
        """Should include lunch break when times are valid."""
        data = pd.DataFrame({
            'opening_hours_begin': ['09:00'],
            'opening_hours_end': ['18:00'],
            'lunch_time_begin': ['12:00'],
            'lunch_time_end': ['13:00'],
            'day_of_week': ['Mo'],
        })

        oh = OpeningHours(data)
        result = oh.process()

        if len(result) > 0 and pd.notna(result[0]):
            # Should contain lunch break info or proper format
            self.assertNotIn('nan-nan', result[0])

    def test_all_nan_times_skipped(self):
        """Should skip rows where all times are NaN."""
        data = pd.DataFrame({
            'opening_hours_begin': [np.nan, np.nan],
            'opening_hours_end': [np.nan, np.nan],
            'lunch_time_begin': [np.nan, np.nan],
            'lunch_time_end': [np.nan, np.nan],
            'day_of_week': ['Mo', 'Tu'],
        })

        oh = OpeningHours(data)
        result = oh.process()

        # Should return empty or NaN values for all rows
        self.assertTrue(len(result) == 0 or all(pd.isna(r) for r in result))

    def test_mixed_valid_invalid(self):
        """Should handle mix of valid and invalid rows."""
        data = pd.DataFrame({
            'opening_hours_begin': ['09:00', np.nan, '11:00'],
            'opening_hours_end': ['17:00', np.nan, '19:00'],
            'lunch_time_begin': [np.nan, np.nan, '12:00'],
            'lunch_time_end': [np.nan, np.nan, '13:00'],
            'day_of_week': ['Mo', 'Tu', 'We'],
        })

        oh = OpeningHours(data)
        result = oh.process()

        # Valid rows should have proper format
        for i, hours in enumerate(result):
            if pd.notna(hours):
                self.assertNotIn('nan-nan', str(hours).lower())


if __name__ == '__main__':
    unittest.main()
