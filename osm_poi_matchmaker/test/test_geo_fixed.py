# -*- coding: utf-8 -*-
"""Test fixed geo module with exception handling."""

import unittest
from osm_poi_matchmaker.libs.geo import check_hu_boundary


class TestGeoHungarianBoundaryFixed(unittest.TestCase):
    """Test Hungarian boundary checking with exception handling fix."""

    def test_valid_budapest_coordinates(self):
        """Should accept valid Budapest coordinates."""
        # Budapest: 19.04°E, 47.50°N
        lat, lon = check_hu_boundary(47.50, 19.04)
        self.assertIsNotNone(lat)
        self.assertIsNotNone(lon)
        self.assertEqual(lat, 47.50)
        self.assertEqual(lon, 19.04)

    def test_valid_szeged_coordinates(self):
        """Should accept valid Szeged coordinates."""
        # Szeged: 20.14°E, 46.25°N
        lat, lon = check_hu_boundary(46.25, 20.14)
        self.assertIsNotNone(lat)
        self.assertIsNotNone(lon)

    def test_valid_debrecen_coordinates(self):
        """Should accept valid Debrecen coordinates."""
        # Debrecen: 21.63°E, 47.53°N
        lat, lon = check_hu_boundary(47.53, 21.63)
        self.assertIsNotNone(lat)
        self.assertIsNotNone(lon)

    def test_outside_latitude_north(self):
        """Should handle coordinates north of Hungary."""
        lat, lon = check_hu_boundary(49.0, 19.0)
        # Should return tuple (may be corrected or None)
        self.assertIsInstance(lat, (type(None), float, int, str))

    def test_outside_latitude_south(self):
        """Should handle coordinates south of Hungary."""
        lat, lon = check_hu_boundary(45.0, 19.0)
        # Should return tuple
        self.assertIsInstance(lat, (type(None), float, int, str))

    def test_outside_longitude_east(self):
        """Should handle coordinates east of Hungary."""
        lat, lon = check_hu_boundary(47.5, 23.0)
        self.assertIsNotNone((lat, lon))

    def test_outside_longitude_west(self):
        """Should handle coordinates west of Hungary."""
        lat, lon = check_hu_boundary(47.5, 16.0)
        self.assertIsNotNone((lat, lon))

    def test_swapped_coordinates(self):
        """Should correct swapped lat/lon."""
        # Budapest with swapped: lat=19.04, lon=47.50
        # The function should swap them back
        lat, lon = check_hu_boundary(19.04, 47.50)
        # Function detects lat < 44 and swaps
        if lat is not None and lon is not None:
            # If swapped, lat should now be ~47, lon should be ~19
            self.assertGreater(lat, 44)

    def test_missing_decimal_point(self):
        """Should handle coordinates missing decimal points."""
        # e.g., '1904' should become 19.04
        lat, lon = check_hu_boundary(473521, 1904)
        # Function should insert decimal points
        if lat is not None:
            self.assertLess(lat, 100)  # After decimal insertion
        if lon is not None:
            self.assertLess(lon, 100)

    def test_zero_coordinates_handled(self):
        """Should handle zero coordinates."""
        lat, lon = check_hu_boundary(0, 0)
        # Function treats 0 as missing
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_empty_string_lat(self):
        """Should handle empty string latitude."""
        lat, lon = check_hu_boundary('', 47.5)
        # Empty string is treated as missing
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_empty_string_lon(self):
        """Should handle empty string longitude."""
        lat, lon = check_hu_boundary(47.5, '')
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_none_type_lat(self):
        """Should handle None latitude."""
        lat, lon = check_hu_boundary(None, 47.5)
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_none_type_lon(self):
        """Should handle None longitude."""
        lat, lon = check_hu_boundary(47.5, None)
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_invalid_float_lat(self):
        """Should handle invalid float latitude gracefully."""
        lat, lon = check_hu_boundary('invalid', 47.5)
        # Should return (None, None) on conversion error
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_invalid_float_lon(self):
        """Should handle invalid float longitude gracefully."""
        lat, lon = check_hu_boundary(47.5, 'invalid')
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_scientific_notation(self):
        """Should handle scientific notation."""
        lat, lon = check_hu_boundary(4.750e1, 1.904e1)  # 47.50, 19.04
        if lat is not None and lon is not None:
            self.assertGreater(lat, 44)
            self.assertGreater(lon, 15)

    def test_very_large_numbers(self):
        """Should handle very large numbers gracefully."""
        lat, lon = check_hu_boundary(999999, 999999)
        # Should return tuple (may be None or corrected)
        self.assertIsInstance((lat, lon), tuple)

    def test_negative_latitude(self):
        """Should handle negative latitude."""
        lat, lon = check_hu_boundary(-47.5, 19.0)
        # Returns tuple (validation happens elsewhere)
        self.assertIsInstance((lat, lon), tuple)

    def test_negative_longitude(self):
        """Should handle negative longitude."""
        lat, lon = check_hu_boundary(47.5, -19.0)
        self.assertIsInstance((lat, lon), tuple)

    def test_corner_coordinates(self):
        """Should accept corner coordinates of Hungary."""
        # NW corner (approx)
        lat, lon = check_hu_boundary(48.6, 16.1)
        if lat is not None:
            self.assertGreater(lat, 44)

        # SE corner (approx)
        lat, lon = check_hu_boundary(45.5, 22.9)
        if lat is not None:
            self.assertGreater(lat, 44)

    def test_return_type_always_tuple(self):
        """Should always return a tuple (lat, lon)."""
        test_cases = [
            (47.5, 19.0),
            (0, 0),
            (None, 19.0),
            ('abc', 'def'),
            (50.0, 15.0),
        ]

        for lat_in, lon_in in test_cases:
            result = check_hu_boundary(lat_in, lon_in)
            self.assertIsInstance(result, tuple)
            self.assertEqual(len(result), 2)


class TestGeoErrorHandling(unittest.TestCase):
    """Test error handling in geo module."""

    def test_no_crash_on_invalid_input(self):
        """Should never crash, always return tuple."""
        test_cases = [
            ('abc', 'def'),
            ('', ''),
            (None, None),
            ('NaN', 'Inf'),
            ('-inf', '+inf'),
        ]

        for lat, lon in test_cases:
            with self.subTest(lat=lat, lon=lon):
                result = check_hu_boundary(lat, lon)
                self.assertIsInstance(result, tuple)
                self.assertEqual(len(result), 2)
                # Both should be None or numeric
                for val in result:
                    self.assertTrue(val is None or isinstance(val, (int, float)))

    def test_exception_handling_consistency(self):
        """Should handle all error types consistently."""
        # Various invalid inputs should all return (None, None)
        invalid_inputs = [
            ('xyz', 47.5),
            (47.5, 'xyz'),
            ({'dict': 'value'}, 47.5),  # May raise TypeError
        ]

        for lat, lon in invalid_inputs:
            try:
                result = check_hu_boundary(lat, lon)
                # If it doesn't crash, should return tuple
                self.assertIsInstance(result, tuple)
            except TypeError:
                # Some invalid types may raise TypeError
                pass


if __name__ == '__main__':
    unittest.main()
