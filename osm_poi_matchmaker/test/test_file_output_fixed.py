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

        filepath = os.path.join(self.test_dir, 'test_comma.csv')
        save_csv_file(df, filepath)

        # Read back and verify
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        # Should have 3 rows (header + 2 data rows)
        self.assertEqual(len(rows), 3)

        # Second row should have 3 columns (not split by embedded comma)
        self.assertEqual(len(rows[1]), 3)
        self.assertIn('Krisztina krt. 51, 1013 Budapest', rows[1][1])

    def test_quoted_field_with_quotes(self):
        """Should escape quotes in quoted fields."""
        df = pd.DataFrame({
            'poi_name': ['Store "Main"', 'Store "North"'],
            'poi_value': [100, 200],
        })

        filepath = os.path.join(self.test_dir, 'test_quotes.csv')
        save_csv_file(df, filepath)

        # Read back
        df_read = pd.read_csv(filepath)

        # Should have correct number of columns
        self.assertEqual(len(df_read.columns), 2)

    def test_field_with_newline(self):
        """Should handle fields with newlines."""
        df = pd.DataFrame({
            'poi_name': ['Store\nLocation', 'Store2'],
            'poi_code': [100, 200],
        })

        filepath = os.path.join(self.test_dir, 'test_newline.csv')
        save_csv_file(df, filepath)

        # Read back
        df_read = pd.read_csv(filepath)
        self.assertEqual(len(df_read), 2)

    def test_all_fields_quoted(self):
        """Should quote all fields with QUOTE_ALL."""
        df = pd.DataFrame({
            'col1': ['value1', 'value2'],
            'col2': ['100', '200'],
            'col3': ['true', 'false'],
        })

        filepath = os.path.join(self.test_dir, 'test_all_quoted.csv')
        save_csv_file(df, filepath)

        # Read raw CSV and check quoting
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Should contain quote characters (QUOTE_ALL)
        self.assertIn('"', content)

        # Each column should be quoted in header
        self.assertIn('"col1"', content)
        self.assertIn('"col2"', content)

    def test_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame()

        filepath = os.path.join(self.test_dir, 'test_empty.csv')
        save_csv_file(df, filepath)

        # File should be created
        self.assertTrue(os.path.exists(filepath))

    def test_numeric_values(self):
        """Should properly quote numeric values."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100.5, 200.3, 300.1],
        })

        filepath = os.path.join(self.test_dir, 'test_numeric.csv')
        save_csv_file(df, filepath)

        df_read = pd.read_csv(filepath)
        self.assertEqual(len(df_read), 3)

    def test_roundtrip_data_integrity(self):
        """Should preserve data after export and re-import."""
        df = pd.DataFrame({
            'poi_name': ['Store 1, Budapest', 'Store 2, Szeged'],
            'poi_address': ['Str. 1, City', 'Str. 2, City'],
            'poi_postcode': ['1011', '6700'],
        })

        filepath = os.path.join(self.test_dir, 'test_roundtrip.csv')
        save_csv_file(df, filepath)

        df_read = pd.read_csv(filepath)

        # Should have same shape
        self.assertEqual(df.shape, df_read.shape)

        # Should have same columns
        self.assertListEqual(list(df.columns), list(df_read.columns))

    def test_special_characters_handling(self):
        """Should handle special characters."""
        df = pd.DataFrame({
            'poi_name': ['Store & Restaurant', 'Café'],
            'poi_symbol': ['@', '#'],
        })

        filepath = os.path.join(self.test_dir, 'test_special.csv')
        save_csv_file(df, filepath)

        df_read = pd.read_csv(filepath)
        self.assertEqual(len(df_read), 2)

    def test_unicode_characters(self):
        """Should handle Unicode characters (Hungarian)."""
        df = pd.DataFrame({
            'poi_name': ['Étterem', 'Fürdő'],
            'poi_city': ['Budapest', 'Eger'],
        })

        filepath = os.path.join(self.test_dir, 'test_unicode.csv')
        save_csv_file(df, filepath)

        df_read = pd.read_csv(filepath)
        self.assertEqual(df_read.iloc[0]['poi_name'], 'Étterem')
        self.assertEqual(df_read.iloc[1]['poi_name'], 'Fürdő')

    def test_large_field_with_comma(self):
        """Should handle large text fields with commas."""
        long_text = 'Product description, line 1, line 2, ' * 10
        df = pd.DataFrame({
            'id': [1],
            'description': [long_text],
        })

        filepath = os.path.join(self.test_dir, 'test_large.csv')
        save_csv_file(df, filepath)

        df_read = pd.read_csv(filepath)
        self.assertEqual(len(df_read), 1)

    def test_null_values_handling(self):
        """Should handle null/NaN values correctly."""
        df = pd.DataFrame({
            'col1': ['val1', None, 'val3'],
            'col2': [1, 2, None],
        })

        filepath = os.path.join(self.test_dir, 'test_null.csv')
        save_csv_file(df, filepath)

        df_read = pd.read_csv(filepath)
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

        filepath = os.path.join(self.test_dir, 'test_regression.csv')
        save_csv_file(df, filepath)

        # Count columns in header
        with open(filepath, 'r') as f:
            header_count = len(f.readline().split(',')) // 2 + 1  # Account for quoting
            first_data = f.readline()

        # Read and verify via pandas
        df_read = pd.read_csv(filepath)
        self.assertEqual(len(df_read.columns), len(df.columns))
        self.assertEqual(len(df_read), len(df))


if __name__ == '__main__':
    unittest.main()
