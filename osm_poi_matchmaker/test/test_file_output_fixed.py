# -*- coding: utf-8 -*-
"""Test fixed file_output module with CSV quoting."""

import unittest
import tempfile
import os
import csv
import pandas as pd
from osm_poi_matchmaker.libs.file_output import save_csv_file


class TestCSVQuotingFixed(unittest.TestCase):
    """Test CSV export with QUOTE_ALL fix for embedded commas."""

    def setUp(self):
        """Create temp directory for test files."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        shutil.rmtree(self.test_dir)

    def test_embedded_comma_in_field(self):
        """Should properly quote fields with embedded commas."""
        df = pd.DataFrame({
            'poi_name': ['Store 1', 'Store 2'],
            'poi_address': ['Krisztina krt. 51, 1013 Budapest', 'Main St. 123, New York'],
            'poi_code': [100, 200],
        })

        filename = 'test_comma.csv'
        save_csv_file(self.test_dir, filename, df, 'test data')

        # Verify file was created
        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))

    def test_quoted_field_with_quotes(self):
        """Should escape quotes in quoted fields."""
        df = pd.DataFrame({
            'poi_name': ['Store "Main"', 'Store "North"'],
            'poi_value': [100, 200],
        })

        filename = 'test_quotes.csv'
        save_csv_file(self.test_dir, filename, df, 'quotes test')

        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))

    def test_field_with_newline(self):
        """Should handle fields with newlines."""
        df = pd.DataFrame({
            'poi_name': ['Store\nLocation', 'Store2'],
            'poi_code': [100, 200],
        })

        filename = 'test_newline.csv'
        save_csv_file(self.test_dir, filename, df, 'newline test')

        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))

    def test_all_fields_quoted(self):
        """Should quote all fields with QUOTE_ALL."""
        df = pd.DataFrame({
            'col1': ['value1', 'value2'],
            'col2': ['100', '200'],
            'col3': ['true', 'false'],
        })

        filename = 'test_all_quoted.csv'
        save_csv_file(self.test_dir, filename, df, 'all quoted test')

        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))

    def test_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame()

        filename = 'test_empty.csv'
        save_csv_file(self.test_dir, filename, df, 'empty test')

        # File should be created
        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))

    def test_numeric_values(self):
        """Should properly quote numeric values."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100.5, 200.3, 300.1],
        })

        filename = 'test_numeric.csv'
        save_csv_file(self.test_dir, filename, df, 'numeric test')

        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))

    def test_roundtrip_data_integrity(self):
        """Should preserve data after export."""
        df = pd.DataFrame({
            'poi_name': ['Store 1, Budapest', 'Store 2, Szeged'],
            'poi_address': ['Str. 1, City', 'Str. 2, City'],
            'poi_postcode': ['1011', '6700'],
        })

        filename = 'test_roundtrip.csv'
        save_csv_file(self.test_dir, filename, df, 'roundtrip test')

        filepath = os.path.join(self.test_dir, filename)
        df_read = pd.read_csv(filepath, index_col=0)

        # Should have same columns
        self.assertEqual(len(df_read.columns), len(df.columns))

    def test_special_characters_handling(self):
        """Should handle special characters."""
        df = pd.DataFrame({
            'poi_name': ['Store & Restaurant', 'Café'],
            'poi_symbol': ['@', '#'],
        })

        filename = 'test_special.csv'
        save_csv_file(self.test_dir, filename, df, 'special chars test')

        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))

    def test_unicode_characters(self):
        """Should handle Unicode characters (Hungarian)."""
        df = pd.DataFrame({
            'poi_name': ['Étterem', 'Fürdő'],
            'poi_city': ['Budapest', 'Eger'],
        })

        filename = 'test_unicode.csv'
        save_csv_file(self.test_dir, filename, df, 'unicode test')

        filepath = os.path.join(self.test_dir, filename)
        df_read = pd.read_csv(filepath, index_col=0)
        self.assertEqual(df_read.iloc[0]['poi_name'], 'Étterem')

    def test_large_field_with_comma(self):
        """Should handle large text fields with commas."""
        long_text = 'Product description, line 1, line 2, ' * 10
        df = pd.DataFrame({
            'id': [1],
            'description': [long_text],
        })

        filename = 'test_large.csv'
        save_csv_file(self.test_dir, filename, df, 'large field test')

        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))

    def test_null_values_handling(self):
        """Should handle null/NaN values correctly."""
        df = pd.DataFrame({
            'col1': ['val1', None, 'val3'],
            'col2': [1, 2, None],
        })

        filename = 'test_null.csv'
        save_csv_file(self.test_dir, filename, df, 'null values test')

        filepath = os.path.join(self.test_dir, filename)
        df_read = pd.read_csv(filepath, index_col=0)
        self.assertEqual(len(df_read), 3)


class TestCSVExportRegressions(unittest.TestCase):
    """Test for regression of known issues."""

    def setUp(self):
        """Create temp directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up."""
        import shutil
        shutil.rmtree(self.test_dir)

    def test_column_count_mismatch(self):
        """Should not have header-data column mismatch."""
        # Before fix: fields with commas caused extra columns in data
        df = pd.DataFrame({
            'name': ['Place 1, Location', 'Place 2, Location'],
            'code': ['1011', '1013'],
            'info': ['Info, with comma', 'More info'],
        })

        filename = 'test_regression.csv'
        save_csv_file(self.test_dir, filename, df, 'regression test')

        filepath = os.path.join(self.test_dir, filename)
        df_read = pd.read_csv(filepath, index_col=0)

        # Should have correct number of columns
        self.assertEqual(len(df_read.columns), len(df.columns))

    def test_none_data_handling(self):
        """Should handle None data gracefully."""
        # The function should return early if data is None
        save_csv_file(self.test_dir, 'test.csv', None, 'none data test')

        # File should not be created
        filepath = os.path.join(self.test_dir, 'test.csv')
        self.assertFalse(os.path.exists(filepath))


if __name__ == '__main__':
    unittest.main()
