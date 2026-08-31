# -*- coding: utf-8 -*-
"""Test fixed geo module with exception handling."""

import unittest
from unittest.mock import patch
import logging
from osm_poi_matchmaker.libs.geo import check_hu_boundary


class TestGeoHungarianBoundaryFixed(unittest.TestCase):
    """Test Hungarian boundary checking with exception handling fix."""

    def test_valid_budapest_coordinates(self):
        """Should accept valid Budapest coordinates."""
        # Budapest: 19.04°E, 47.50°N
        result = check_hu_boundary('19.04', '47.50')
        self.assertTrue(result)

    def test_valid_szeged_coordinates(self):
        """Should accept valid Szeged coordinates."""
        # Szeged: 20.14°E, 46.25°N
        result = check_hu_boundary('20.14', '46.25')
        self.assertTrue(result)

    def test_valid_debrecen_coordinates(self):
        """Should accept valid Debrecen coordinates."""
        # Debrecen: 21.63°E, 47.53°N
        result = check_hu_boundary('21.63', '47.53')
        self.assertTrue(result)

    def test_outside_latitude_north(self):
        """Should reject coordinates north of Hungary."""
        result = check_hu_boundary('19.0', '49.0')
        self.assertFalse(result)

    def test_outside_latitude_south(self):
        """Should reject coordinates south of Hungary."""
        result = check_hu_boundary('19.0', '45.0')
        self.assertFalse(result)

    def test_outside_longitude_east(self):
        """Should reject coordinates east of Hungary."""
        result = check_hu_boundary('23.0', '47.5')
        self.assertFalse(result)

    def test_outside_longitude_west(self):
        """Should reject coordinates west of Hungary."""
        result = check_hu_boundary('16.0', '47.5')
        self.assertFalse(result)

    def test_boundary_min_latitude(self):
        """Should accept minimum latitude boundary."""
        result = check_hu_boundary('19.0', '45.5')
        self.assertTrue(result)

    def test_boundary_max_latitude(self):
        """Should accept maximum latitude boundary."""
        result = check_hu_boundary('19.0', '48.6')
        self.assertTrue(result)

    def test_boundary_min_longitude(self):
        """Should accept minimum longitude boundary."""
        result = check_hu_boundary('16.1', '47.5')
        self.assertTrue(result)

    def test_boundary_max_longitude(self):
        """Should accept maximum longitude boundary."""
        result = check_hu_boundary('22.9', '47.5')
        self.assertTrue(result)

    def test_invalid_float_lon(self):
        """Should handle invalid float longitude gracefully."""
        # Before fix: could crash on float() conversion
        with patch('osm_poi_matchmaker.libs.geo.logging') as mock_logging:
            result = check_hu_boundary('invalid', '47.5')
            self.assertFalse(result)

    def test_invalid_float_lat(self):
        """Should handle invalid float latitude gracefully."""
        # Before fix: could crash on float() conversion
        result = check_hu_boundary('19.0', 'invalid')
        self.assertFalse(result)

    def test_missing_decimal_point_lon(self):
        """Should handle coordinates missing decimal points."""
        # Before fix: could fail on string slicing if decimal not found
        result = check_hu_boundary('1904', '4750')
        # These are out of bounds without decimal, so should be False
        self.assertFalse(result)

    def test_scientific_notation(self):
        """Should handle scientific notation."""
        result = check_hu_boundary('1.904e1', '4.750e1')
        self.assertTrue(result)  # 19.04, 47.50

    def test_negative_longitude(self):
        """Should reject negative longitude."""
        result = check_hu_boundary('-19.0', '47.5')
        self.assertFalse(result)

    def test_negative_latitude(self):
        """Should reject negative latitude."""
        result = check_hu_boundary('19.0', '-47.5')
        self.assertFalse(result)

    def test_zero_coordinates(self):
        """Should reject zero coordinates."""
        result = check_hu_boundary('0', '0')
        self.assertFalse(result)

    def test_very_large_numbers(self):
        """Should handle very large numbers gracefully."""
        result = check_hu_boundary('999999', '999999')
        self.assertFalse(result)

    def test_empty_string_lon(self):
        """Should handle empty string longitude."""
        result = check_hu_boundary('', '47.5')
        self.assertFalse(result)

    def test_empty_string_lat(self):
        """Should handle empty string latitude."""
        result = check_hu_boundary('19.0', '')
        self.assertFalse(result)

    def test_none_type_lon(self):
        """Should handle None longitude."""
        result = check_hu_boundary(None, '47.5')
        self.assertFalse(result)

    def test_none_type_lat(self):
        """Should handle None latitude."""
        result = check_hu_boundary('19.0', None)
        self.assertFalse(result)

    def test_whitespace_in_coordinates(self):
        """Should handle whitespace in coordinates."""
        # Might strip whitespace or reject
        result = check_hu_boundary(' 19.0 ', ' 47.5 ')
        # Depends on implementation - at least shouldn't crash
        self.assertIsNotNone(result)

    def test_swapped_coordinates(self):
        """Should reject swapped lat/lon."""
        # Budapest with swapped: lat=19.04, lon=47.50
        result = check_hu_boundary('47.50', '19.04')
        self.assertFalse(result)  # lat should be 45.5-48.6, lon 16.1-22.9

    def test_corner_coordinates(self):
        """Should accept all four corners of Hungary."""
        # NW corner (approx)
        self.assertTrue(check_hu_boundary('16.1', '48.6'))
        # NE corner (approx)
        self.assertTrue(check_hu_boundary('22.9', '48.6'))
        # SW corner (approx)
        self.assertTrue(check_hu_boundary('16.1', '45.5'))
        # SE corner (approx)
        self.assertTrue(check_hu_boundary('22.9', '45.5'))


class TestGeoErrorHandling(unittest.TestCase):
    """Test error handling in geo module."""

    def test_no_crash_on_invalid_input(self):
        """Should never crash, always return boolean."""
        test_cases = [
            ('abc', 'def'),
            ('', ''),
            (None, None),
            ('NaN', 'Inf'),
            ('-inf', '+inf'),
        ]

        for lon, lat in test_cases:
            with self.subTest(lon=lon, lat=lat):
                result = check_hu_boundary(lon, lat)
                self.assertIsInstance(result, bool)


if __name__ == '__main__':
    unittest.main()
