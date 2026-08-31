# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import pandas as pd
    import re
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)


class ValidationError:
    """Represents a single data validation error."""

    def __init__(self, row_index, field, value, rule, message):
        self.row_index = row_index
        self.field = field
        self.value = value
        self.rule = rule
        self.message = message

    def __repr__(self):
        return f"Row {self.row_index}: {self.field}={self.value} ({self.message})"


class POIDataValidator:
    """Validates POI data against business rules."""

    HUNGARIAN_LAT_MIN = 45.5
    HUNGARIAN_LAT_MAX = 48.6
    HUNGARIAN_LON_MIN = 16.1
    HUNGARIAN_LON_MAX = 22.9

    POSTCODE_PATTERN = re.compile(r'^\d{4}$')
    OPENING_HOURS_PATTERN = re.compile(r'^[A-Za-z0-9\-\;:,\s]+$')

    def __init__(self):
        self.errors = []

    def validate(self, data, strict=False):
        """Run all validation rules on the dataset.

        Args:
            data (pd.DataFrame): POI data to validate
            strict (bool): If True, fail on first error. If False, collect all errors.

        Returns:
            tuple: (is_valid, errors_list)
        """
        self.errors = []

        for idx, row in data.iterrows():
            try:
                self._validate_row(row, idx, strict)
            except Exception as e:
                logging.warning('Validation error at row %d: %s', idx, e)

        return len(self.errors) == 0, self.errors

    def _validate_row(self, row, idx, strict=False):
        """Validate a single row."""
        # Required fields
        if pd.isna(row.get('poi_lon')) or pd.isna(row.get('poi_lat')):
            self._add_error(idx, 'poi_lon/poi_lat', None, 'required', 'Missing coordinates')
            if strict:
                raise ValueError('Missing coordinates')

        # Coordinate bounds
        if row.get('poi_lat') is not None and row.get('poi_lon') is not None:
            lat = float(row['poi_lat'])
            lon = float(row['poi_lon'])
            if not (self.HUNGARIAN_LAT_MIN <= lat <= self.HUNGARIAN_LAT_MAX):
                self._add_error(idx, 'poi_lat', lat, 'hungarian_bounds', f'Latitude {lat} outside Hungary')
            if not (self.HUNGARIAN_LON_MIN <= lon <= self.HUNGARIAN_LON_MAX):
                self._add_error(idx, 'poi_lon', lon, 'hungarian_bounds', f'Longitude {lon} outside Hungary')

        # Postcode validation (only if present)
        if row.get('poi_postcode') not in (None, '', 'None'):
            postcode_str = str(row['poi_postcode']).strip()
            if not self.POSTCODE_PATTERN.match(postcode_str):
                self._add_error(idx, 'poi_postcode', postcode_str, 'postcode_format',
                               f'Postcode must be 4 digits, got: {postcode_str}')

        # OSM ID validation (only if present)
        if pd.notna(row.get('osm_id')):
            osm_id = row['osm_id']
            if not isinstance(osm_id, int) or osm_id <= 0:
                self._add_error(idx, 'osm_id', osm_id, 'osm_id_format', f'Invalid OSM ID: {osm_id}')

        # Opening hours format (if present)
        if pd.notna(row.get('poi_opening_hours')):
            oh = str(row['poi_opening_hours'])
            if not self.OPENING_HOURS_PATTERN.match(oh):
                self._add_error(idx, 'poi_opening_hours', oh, 'opening_hours_format',
                               f'Invalid format: {oh}')

    def _add_error(self, row_index, field, value, rule, message):
        """Add a validation error."""
        error = ValidationError(row_index, field, value, rule, message)
        self.errors.append(error)
        logging.debug(error)

    def get_summary(self):
        """Get validation error summary."""
        if not self.errors:
            return {'valid': True, 'error_count': 0}

        by_rule = {}
        for err in self.errors:
            by_rule.setdefault(err.rule, 0)
            by_rule[err.rule] += 1

        return {
            'valid': False,
            'error_count': len(self.errors),
            'errors_by_rule': by_rule,
            'error_rate_percent': (len(self.errors) / len(self.errors)) * 100 if self.errors else 0
        }


def validate_poi_dataset(data, strict=False):
    """Convenience function to validate POI dataset.

    Args:
        data (pd.DataFrame): POI data
        strict (bool): Fail on first error

    Returns:
        tuple: (is_valid, errors, summary)
    """
    validator = POIDataValidator()
    is_valid, errors = validator.validate(data, strict)
    summary = validator.get_summary()

    logging.info('Validation complete: %s', summary)
    if errors:
        for err in errors[:10]:
            logging.warning(err)
        if len(errors) > 10:
            logging.warning('... and %d more errors', len(errors) - 10)

    return is_valid, errors, summary
