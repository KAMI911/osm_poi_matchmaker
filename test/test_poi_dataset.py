# -*- coding: utf-8 -*-

try:
    import unittest
    from osm_poi_matchmaker.libs.poi_dataset import POIDataset
    from test.test_opening_hours_data import OPENING_HOURS_TEST_DATA
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


class TestPOIDataset(unittest.TestCase):
    def setUp(self):
        self.opening_hours = OPENING_HOURS_TEST_DATA

    def test_poi_dataset_opening_hours(self):
        for i in self.opening_hours:
            p = POIDataset()
            p.nonstop = i['nonstop']
            p.mo_o = i['mo_o']
            p.tu_o = i['tu_o']
            p.we_o = i['we_o']
            p.th_o = i['th_o']
            p.fr_o = i['fr_o']
            p.sa_o = i['sa_o']
            p.su_o = i['su_o']
            p.mo_c = i['mo_c']
            p.tu_c = i['tu_c']
            p.we_c = i['we_c']
            p.th_c = i['th_c']
            p.fr_c = i['fr_c']
            p.sa_c = i['sa_c']
            p.su_c = i['su_c']
            p.summer_mo_o = i['summer_mo_o']
            p.summer_tu_o = i['summer_tu_o']
            p.summer_we_o = i['summer_we_o']
            p.summer_th_o = i['summer_th_o']
            p.summer_fr_o = i['summer_fr_o']
            p.summer_sa_o = i['summer_sa_o']
            p.summer_su_o = i['summer_su_o']
            p.summer_mo_c = i['summer_mo_c']
            p.summer_tu_c = i['summer_tu_c']
            p.summer_we_c = i['summer_we_c']
            p.summer_th_c = i['summer_th_c']
            p.summer_fr_c = i['summer_fr_c']
            p.summer_sa_c = i['summer_sa_c']
            p.summer_su_c = i['summer_su_c']
            p.lunch_break_start = i['lunch_break_start']
            p.lunch_break_stop = i['lunch_break_stop']
            p.public_holiday_open = i['public_holiday_open']
            with self.subTest():
                p.process_opening_hours()
                self.assertEqual(i['processed'], p.opening_hours)
