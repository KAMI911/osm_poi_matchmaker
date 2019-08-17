# -*- coding: utf-8 -*-

try:
    import unittest
    from libs.opening_hours import OpeningHours
    from test.test_opening_hours_data import OPENING_HOURS_TEST_DATA
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


class TestOpeningHours(unittest.TestCase):
    def setUp(self):
        self.opening_hours = OPENING_HOURS_TEST_DATA

    def test_extract_opening_hours(self):
        for i in self.opening_hours:
            if 'public_holiday_open' not in i:
                i['public_holiday_open'] = None
            p = OpeningHours(i['nonstop'], i['mo_o'], i['tu_o'], i['we_o'], i['th_o'], i['fr_o'], i['sa_o'],
                             i['su_o'], i['mo_c'], i['tu_c'], i['we_c'], i['th_c'], i['fr_c'], i['sa_c'],
                             i['su_c'], i['summer_mo_o'], i['summer_tu_o'], i['summer_we_o'], i['summer_th_o'],
                             i['summer_fr_o'], i['summer_sa_o'], i['summer_su_o'], i['summer_mo_c'],
                             i['summer_tu_c'], i['summer_we_c'], i['summer_th_c'], i['summer_fr_c'],
                             i['summer_sa_c'], i['summer_su_c'], i['lunch_break_start'], i['lunch_break_stop'],
                             i['public_holiday_open'])
            with self.subTest():
                self.assertEqual(i['processed'], p.process())
