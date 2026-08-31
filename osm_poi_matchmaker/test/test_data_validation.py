# -*- coding: utf-8 -*-

import unittest
import pandas as pd
import numpy as np
from osm_poi_matchmaker.libs.data_validation import (
    ValidationError, POIDataValidator, validate_poi_dataset
)


class TestValidationError(unittest.TestCase):
    """Test ValidationError class."""

    def test_error_repr(self):
        """Should format error message."""
        err = ValidationError(0, 'poi_name', 'Test', 'required', 'Name missing')
        self.assertIn('Row 0', str(err))
        self.assertIn('poi_name', str(err))


class TestPOIDataValidator(unittest.TestCase):
    """Test POI data validator."""

    def setUp(self):
        """Create valid base DataFrame."""
        self.df = pd.DataFrame({
            'poi_lon': [19.0, 19.1, 19.2],
            'poi_lat': [47.5, 47.6, 47.7],
            'poi_postcode': ['1011', '1013', '1015'],
            'osm_id': [100, 200, 300],
            'poi_opening_hours': ['Mo-Su 08:00-20:00', 'Mo-Fr 09:00-17:00', None],
        })

    def test_valid_dataset(self):
        """Should accept valid data."""
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

    def test_missing_coordinates(self):
        """Should catch missing lat/lon."""
        self.df.loc[0, 'poi_lat'] = np.nan
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)
        self.assertGreaterEqual(len(errors), 1)
        self.assertTrue(any(e.rule == 'required' for e in errors))

    def test_missing_lon(self):
        """Should catch missing longitude."""
        self.df.loc[1, 'poi_lon'] = None
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)

    def test_coordinates_outside_hungary(self):
        """Should reject coordinates outside Hungarian bounds."""
        self.df.loc[0, 'poi_lat'] = 50.0
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)
        self.assertEqual(errors[0].rule, 'hungarian_bounds')

    def test_coordinates_lon_outside(self):
        """Should reject longitude outside bounds."""
        self.df.loc[1, 'poi_lon'] = 23.0
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)

    def test_valid_postcode_formats(self):
        """Should accept valid 4-digit postcodes."""
        self.df['poi_postcode'] = ['1011', '2600', '9700']
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertTrue(is_valid)

    def test_invalid_postcode_too_long(self):
        """Should reject postcodes longer than 4 digits."""
        self.df.loc[0, 'poi_postcode'] = '10110'
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)
        self.assertEqual(errors[0].rule, 'postcode_format')

    def test_invalid_postcode_too_short(self):
        """Should reject postcodes shorter than 4 digits."""
        self.df.loc[1, 'poi_postcode'] = '101'
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)

    def test_invalid_postcode_non_numeric(self):
        """Should reject non-numeric postcodes."""
        self.df.loc[2, 'poi_postcode'] = 'ABCD'
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)

    def test_missing_postcode_is_ok(self):
        """Should allow missing postcodes."""
        self.df.loc[0, 'poi_postcode'] = None
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertTrue(is_valid)

    def test_empty_postcode_is_ok(self):
        """Should allow empty postcodes."""
        self.df.loc[0, 'poi_postcode'] = ''
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertTrue(is_valid)

    def test_osm_id_validation_valid(self):
        """Should accept positive integer OSM IDs."""
        self.df['osm_id'] = [100, 200, 300]
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertTrue(is_valid)

    def test_osm_id_validation_negative(self):
        """Should reject negative OSM IDs."""
        self.df.loc[0, 'osm_id'] = -100
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)
        self.assertEqual(errors[0].rule, 'osm_id_format')

    def test_osm_id_validation_zero(self):
        """Should reject zero OSM ID."""
        self.df.loc[1, 'osm_id'] = 0
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertFalse(is_valid)

    def test_osm_id_missing_is_ok(self):
        """Should allow missing OSM IDs."""
        self.df.loc[0, 'osm_id'] = None
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertTrue(is_valid)

    def test_opening_hours_valid_formats(self):
        """Should accept valid opening hours."""
        self.df['poi_opening_hours'] = [
            'Mo-Su 08:00-20:00',
            'Mo-Fr 09:00-17:00; Sa 09:00-13:00',
            'Mo-We 10:00-18:00,20:00-22:00'
        ]
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertTrue(is_valid)

    def test_opening_hours_missing_is_ok(self):
        """Should allow missing opening hours."""
        self.df['poi_opening_hours'] = [None, None, None]
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df)
        self.assertTrue(is_valid)

    def test_multiple_errors_collected(self):
        """Should collect all errors in non-strict mode."""
        self.df.loc[0, 'poi_lat'] = 50.0
        self.df.loc[1, 'poi_postcode'] = 'XXXX'
        self.df.loc[2, 'osm_id'] = -1
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df, strict=False)
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)

    def test_strict_mode_stops_first_error(self):
        """Should stop at first error in strict mode."""
        self.df.loc[0, 'poi_lat'] = 50.0
        self.df.loc[1, 'poi_postcode'] = 'XXXX'
        validator = POIDataValidator()
        is_valid, errors = validator.validate(self.df, strict=True)
        self.assertFalse(is_valid)

    def test_get_summary(self):
        """Should return error summary."""
        self.df.loc[0, 'poi_postcode'] = 'BAD'
        self.df.loc[1, 'osm_id'] = -1
        validator = POIDataValidator()
        validator.validate(self.df)
        summary = validator.get_summary()
        self.assertFalse(summary['valid'])
        self.assertIn('error_count', summary)
        self.assertIn('errors_by_rule', summary)

    def test_hungarian_bounds_edge_cases(self):
        """Should validate Hungarian boundary coordinates."""
        validator = POIDataValidator()

        df_min_lat = self.df.copy()
        df_min_lat.loc[0, 'poi_lat'] = 45.5
        is_valid, _ = validator.validate(df_min_lat)
        self.assertTrue(is_valid)

        df_max_lat = self.df.copy()
        df_max_lat.loc[0, 'poi_lat'] = 48.6
        is_valid, _ = validator.validate(df_max_lat)
        self.assertTrue(is_valid)

        df_below_lat = self.df.copy()
        df_below_lat.loc[0, 'poi_lat'] = 45.4
        is_valid, _ = validator.validate(df_below_lat)
        self.assertFalse(is_valid)


class TestValidatePoiDataset(unittest.TestCase):
    """Test convenience function."""

    def test_validate_poi_dataset_valid(self):
        """Should return (True, [], valid_summary) for valid data."""
        df = pd.DataFrame({
            'poi_lon': [19.0],
            'poi_lat': [47.5],
            'poi_postcode': ['1011'],
            'osm_id': [100],
        })
        is_valid, errors, summary = validate_poi_dataset(df)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
        self.assertTrue(summary['valid'])

    def test_validate_poi_dataset_invalid(self):
        """Should return (False, [errors], invalid_summary) for bad data."""
        df = pd.DataFrame({
            'poi_lon': [None],
            'poi_lat': [47.5],
        })
        is_valid, errors, summary = validate_poi_dataset(df)
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
        self.assertFalse(summary['valid'])


if __name__ == '__main__':
    unittest.main()
