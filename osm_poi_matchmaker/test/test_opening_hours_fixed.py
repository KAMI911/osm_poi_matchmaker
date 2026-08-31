# -*- coding: utf-8 -*-
"""Test fixed opening_hours module with nan-nan handling."""

import unittest
import pandas as pd
import numpy as np
from osm_poi_matchmaker.libs.opening_hours import OpeningHours


class TestOpeningHoursNaNHandling(unittest.TestCase):
    """Test nan-nan problem fix in opening_hours."""

    def test_valid_opening_hours_no_crash(self):
        """Should not crash with valid opening hours data."""
        # OpeningHours expects 28-30 parameters
        oh = OpeningHours(
            False,  # non_stop
            '09:00', '09:00', '09:00', '09:00', '09:00', '09:00', '09:00',  # mo-su open
            '17:00', '17:00', '17:00', '17:00', '17:00', '17:00', '17:00',  # mo-su close
            None, None, None, None, None, None, None,  # summer open
            None, None, None, None, None, None, None,  # summer close
            None, None  # lunch break
        )
        result = oh.process()
        # Should return string, not contain "nan-nan"
        if isinstance(result, str):
            self.assertNotIn('nan-nan', result.lower())

    def test_no_nan_in_output(self):
        """Should not produce nan-nan in output."""
        oh = OpeningHours(
            False,  # non_stop
            '10:00', '10:00', '10:00', '10:00', '10:00', '10:00', '10:00',  # mo-su open
            '18:00', '18:00', '18:00', '18:00', '18:00', '18:00', '18:00',  # mo-su close
            None, None, None, None, None, None, None,  # summer open
            None, None, None, None, None, None, None,  # summer close
            None, None  # lunch break
        )
        result = oh.process()

        if result is not None:
            result_str = str(result)
            # Should not contain "nan-nan" pattern
            self.assertNotIn('nan-nan', result_str.lower())
            # Should not contain "None-None"
            self.assertNotIn('None-None', result_str)

    def test_nonstop_handling(self):
        """Should handle non-stop hours."""
        oh = OpeningHours(
            True,  # non_stop=True
            None, None, None, None, None, None, None,
            None, None, None, None, None, None, None,
            None, None, None, None, None, None, None,
            None, None, None, None, None, None, None,
            None, None
        )
        result = oh.process()
        # Should return valid output
        self.assertIsNotNone(result)

    def test_with_lunch_break(self):
        """Should handle lunch breaks."""
        oh = OpeningHours(
            False,  # non_stop
            '09:00', '09:00', '09:00', '09:00', '09:00', '09:00', '09:00',
            '17:00', '17:00', '17:00', '17:00', '17:00', '17:00', '17:00',
            None, None, None, None, None, None, None,
            None, None, None, None, None, None, None,
            '12:00',  # lunch_break_start
            '13:00'   # lunch_break_end
        )
        result = oh.process()
        # Should not crash and return valid output
        self.assertIsNotNone(result)

    def test_partial_open_hours(self):
        """Should handle partial opening hours (not all days)."""
        oh = OpeningHours(
            False,
            '09:00', None, None, None, None, '09:00', '10:00',  # mo-su open (sparse)
            '17:00', None, None, None, None, '17:00', '18:00',  # mo-su close (sparse)
            None, None, None, None, None, None, None,
            None, None, None, None, None, None, None,
            None, None
        )
        result = oh.process()
        # Should handle sparse data
        self.assertIsNotNone(result)

    def test_process_returns_string_or_none(self):
        """process() should return string or None, never nan-nan."""
        test_cases = [
            # (non_stop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c, fr_c, sa_c, su_c, ...)
            (False, '09:00', '09:00', '09:00', '09:00', '09:00', '09:00', '09:00',
             '17:00', '17:00', '17:00', '17:00', '17:00', '17:00', '17:00',
             None, None, None, None, None, None, None,
             None, None, None, None, None, None, None, None, None),
            (True, None, None, None, None, None, None, None,
             None, None, None, None, None, None, None,
             None, None, None, None, None, None, None,
             None, None, None, None, None, None, None, None, None),
        ]

        for params in test_cases:
            oh = OpeningHours(*params)
            result = oh.process()

            # Result should be string, list, or None
            self.assertTrue(result is None or isinstance(result, (str, list)))

            # Should never contain nan-nan
            if isinstance(result, str):
                self.assertNotIn('nan-nan', result.lower())
            if isinstance(result, list):
                for item in result:
                    if isinstance(item, str):
                        self.assertNotIn('nan-nan', item.lower())


if __name__ == '__main__':
    unittest.main()
