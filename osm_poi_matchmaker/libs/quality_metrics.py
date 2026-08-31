# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import pandas as pd
    import numpy as np
    from datetime import datetime
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)


class QualityMetrics:
    """Calculate and track data quality metrics for POI dataset."""

    def __init__(self, data, stage_name=''):
        self.data = data
        self.stage_name = stage_name
        self.timestamp = datetime.now()
        self.metrics = {}

    def calculate(self):
        """Calculate all quality metrics."""
        self.metrics = {
            'timestamp': self.timestamp.isoformat(),
            'stage': self.stage_name,
            'total_records': len(self.data),
            'valid_records': self._count_valid_records(),
            'missing_coordinates': self._count_missing_coordinates(),
            'missing_postcode': self._count_missing_postcode(),
            'missing_name': self._count_missing_name(),
            'duplicates': self._count_duplicates(),
            'osm_matched': self._count_osm_matched(),
            'osm_new': self._count_osm_new(),
            'errors': self._count_errors(),
        }

        self.metrics['error_rate_percent'] = (
            self.metrics['errors'] / self.metrics['total_records'] * 100
            if self.metrics['total_records'] > 0 else 0
        )

        self.metrics['completion_rate_percent'] = (
            self.metrics['valid_records'] / self.metrics['total_records'] * 100
            if self.metrics['total_records'] > 0 else 0
        )

        return self.metrics

    def _count_valid_records(self):
        """Count records with essential fields."""
        valid = (
            (self.data['poi_lon'].notna()) &
            (self.data['poi_lat'].notna()) &
            (self.data['poi_name'].notna() | self.data['poi_city'].notna())
        ).sum()
        return int(valid)

    def _count_missing_coordinates(self):
        """Count records with missing lat/lon."""
        missing = (
            (self.data['poi_lon'].isna()) |
            (self.data['poi_lat'].isna())
        ).sum()
        return int(missing)

    def _count_missing_postcode(self):
        """Count records with missing postcode."""
        missing = (
            (self.data['poi_postcode'].isna()) |
            (self.data['poi_postcode'] == '') |
            (self.data['poi_postcode'] == 'None')
        ).sum()
        return int(missing)

    def _count_missing_name(self):
        """Count records with missing name."""
        missing = (
            (self.data['poi_name'].isna()) |
            (self.data['poi_name'] == '')
        ).sum()
        return int(missing)

    def _count_duplicates(self):
        """Count records sharing same osm_id."""
        osm_duplicates = (
            self.data[self.data['osm_id'].notna()].groupby('osm_id').size() > 1
        ).sum()

        name_addr_duplicates = (
            self.data.groupby(['poi_name', 'poi_addr_street', 'poi_addr_housenumber']).size() > 1
        ).sum()

        return int(osm_duplicates + name_addr_duplicates)

    def _count_osm_matched(self):
        """Count records with OSM match."""
        matched = self.data['osm_id'].notna().sum()
        return int(matched)

    def _count_osm_new(self):
        """Count new POIs (not matched to OSM)."""
        if 'poi_new' in self.data.columns:
            new = (self.data['poi_new'] == True).sum()
            return int(new)
        return 0

    def _count_errors(self):
        """Count records with match errors."""
        if 'match_error' in self.data.columns:
            errors = (self.data['match_error'] == True).sum()
            return int(errors)
        return 0

    def get_summary(self):
        """Get human-readable summary."""
        if not self.metrics:
            self.calculate()

        return {
            'stage': self.metrics['stage'],
            'total': self.metrics['total_records'],
            'valid': f"{self.metrics['valid_records']} ({self.metrics['completion_rate_percent']:.1f}%)",
            'osm_matched': f"{self.metrics['osm_matched']} ({self.metrics['osm_matched']/self.metrics['total_records']*100:.1f}%)",
            'osm_new': f"{self.metrics['osm_new']} ({self.metrics['osm_new']/self.metrics['total_records']*100 if self.metrics['total_records'] > 0 else 0:.1f}%)",
            'errors': f"{self.metrics['errors']} ({self.metrics['error_rate_percent']:.1f}%)",
            'missing_coords': self.metrics['missing_coordinates'],
            'missing_postcode': self.metrics['missing_postcode'],
            'missing_name': self.metrics['missing_name'],
            'duplicates': self.metrics['duplicates'],
        }

    def log_summary(self):
        """Log quality metrics summary."""
        summary = self.get_summary()
        lines = [
            f"=== Quality Metrics: {self.stage_name} ===",
            f"Total records: {summary['total']}",
            f"Valid records: {summary['valid']}",
            f"OSM matched: {summary['osm_matched']}",
            f"OSM new: {summary['osm_new']}",
            f"Errors: {summary['errors']}",
            f"Missing coordinates: {summary['missing_coords']}",
            f"Missing postcode: {summary['missing_postcode']}",
            f"Missing name: {summary['missing_name']}",
            f"Duplicates: {summary['duplicates']}",
        ]
        logging.info('\n'.join(lines))

    def export_json(self):
        """Export metrics as JSON-serializable dict."""
        if not self.metrics:
            self.calculate()
        return self.metrics


def calculate_quality_metrics(data, stage_name=''):
    """Convenience function to calculate quality metrics.

    Args:
        data (pd.DataFrame): POI data
        stage_name (str): Name of the stage for logging

    Returns:
        dict: Quality metrics dictionary
    """
    calculator = QualityMetrics(data, stage_name)
    metrics = calculator.calculate()
    calculator.log_summary()
    return metrics
