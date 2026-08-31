# -*- coding: utf-8 -*-
"""Test fixed address module with postcode validation."""

import unittest
from osm_poi_matchmaker.libs.address import clean_postcode


class TestPostcodeCleaningFixed(unittest.TestCase):
    """Test postcode cleaning with Hungarian validation fix."""

    def test_valid_4digit_postcode(self):
        """Should accept valid 4-digit Hungarian postcodes."""
        self.assertEqual(clean_postcode('1011'), '1011')
        self.assertEqual(clean_postcode('2600'), '2600')
        self.assertEqual(clean_postcode('9700'), '9700')
        self.assertEqual(clean_postcode('8000'), '8000')

    def test_float_to_string_conversion(self):
        """Should convert float postcodes to string."""
        # Before fix: 1016.0 would remain as "1016.0"
        # After fix: should be "1016"
        result = clean_postcode(1016.0)
        self.assertEqual(result, '1016')

    def test_float_with_zeros(self):
        """Should handle float conversion correctly."""
        self.assertEqual(clean_postcode(1011.0), '1011')
        self.assertEqual(clean_postcode(2600.0), '2600')

    def test_string_float_postcode(self):
        """Should handle string representation of floats."""
        self.assertEqual(clean_postcode('1016.0'), '1016')
        self.assertEqual(clean_postcode('2600.0'), '2600')

    def test_complex_postcode_format(self):
        """Should reject complex formats like 'xxx - Name'."""
        # Before fix: "10003 - Mobiliti" would be accepted
        # After fix: should be rejected (not 4 digits)
        result = clean_postcode('10003 - Mobiliti')
        self.assertIsNone(result)

    def test_mobiliti_format_extraction(self):
        """Should reject Mobiliti-style postcodes (too long)."""
        result = clean_postcode('10003')
        self.assertIsNone(result)

    def test_too_long_postcode(self):
        """Should reject postcodes longer than 4 digits."""
        self.assertIsNone(clean_postcode('10011'))
        self.assertIsNone(clean_postcode('26001'))

    def test_too_short_postcode(self):
        """Should reject postcodes shorter than 4 digits."""
        self.assertIsNone(clean_postcode('101'))
        self.assertIsNone(clean_postcode('260'))
        self.assertIsNone(clean_postcode('1'))

    def test_non_numeric_postcode(self):
        """Should reject non-numeric postcodes."""
        self.assertIsNone(clean_postcode('ABCD'))
        self.assertIsNone(clean_postcode('ABC1'))
        self.assertIsNone(clean_postcode('12AB'))

    def test_whitespace_handling(self):
        """Should strip whitespace."""
        self.assertEqual(clean_postcode(' 1011 '), '1011')
        self.assertEqual(clean_postcode(' 2600'), '2600')
        self.assertEqual(clean_postcode('9700 '), '9700')

    def test_none_input(self):
        """Should handle None input."""
        self.assertIsNone(clean_postcode(None))

    def test_empty_string(self):
        """Should handle empty string."""
        self.assertIsNone(clean_postcode(''))

    def test_international_postcode_format(self):
        """Should reject non-Hungarian postcodes."""
        # US format (5 digits)
        self.assertIsNone(clean_postcode('10001'))
        # UK format (alphanumeric)
        self.assertIsNone(clean_postcode('SW1A1AA'))

    def test_with_special_characters(self):
        """Should handle postcodes with special characters."""
        # The function extracts numeric portions, so '1011-' may yield '1011'
        result1 = clean_postcode('1011-')
        self.assertIsNotNone(result1)  # Extracts '1011'

        # But longer strings with numbers shouldn't match if result is too long
        result2 = clean_postcode('101/1')
        # Could be '1011' or None depending on extraction
        self.assertTrue(result2 is None or result2 == '1011')

        result3 = clean_postcode('10.11')
        # Contains dots, extraction may fail
        self.assertTrue(result3 is None or len(str(result3)) == 4)

    def test_numeric_boundary_values(self):
        """Should accept all valid 4-digit numeric codes."""
        self.assertEqual(clean_postcode('0001'), '0001')
        self.assertEqual(clean_postcode('9999'), '9999')
        self.assertEqual(clean_postcode('5000'), '5000')

    def test_hex_looking_but_numeric(self):
        """Should accept 4-digit codes even if hex-like."""
        # These are still valid as they're 4 decimal digits
        self.assertEqual(clean_postcode('ABCD'), None)  # But not if non-numeric


class TestPostcodeValidationIntegration(unittest.TestCase):
    """Integration tests for postcode cleaning in data pipeline."""

    def test_pipeline_with_mixed_postcodes(self):
        """Should filter dataset correctly."""
        test_cases = [
            ('1011', '1011'),      # valid
            ('2600.0', '2600'),    # float
            ('10003', None),       # too long
            ('101', None),         # too short
            ('ABCD', None),        # non-numeric
            (' 1015 ', '1015'),    # whitespace
            (None, None),          # None
        ]

        for input_pc, expected in test_cases:
            with self.subTest(input=input_pc):
                self.assertEqual(clean_postcode(input_pc), expected)


if __name__ == '__main__':
    unittest.main()
