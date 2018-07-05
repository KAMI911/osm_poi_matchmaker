# -*- coding: utf-8 -*-

try:
    import unittest
    from osm_poi_matchmaker.libs.opening_hours import OpeningHours
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


class TestOpeningHours(unittest.TestCase):
    def setUp(self):
        self.opening_hours = [
            {'nonstop': True, 'mo_o': None, 'tu_o': None, 'we_o': None, 'th_o': None, 'fr_o': None, 'sa_o': None,
             'su_o': None, 'mo_c': None, 'tu_c': None, 'we_c': None, 'th_c': None, 'fr_c': None, 'sa_c': None,
             'su_c': None, 'summer_mo_o': None, 'summer_tu_o': None, 'summer_we_o': None, 'summer_th_o': None,
             'summer_fr_o': None, 'summer_sa_o': None, 'summer_su_o': None, 'summer_mo_c': None, 'summer_tu_c': None,
             'summer_we_c': None, 'summer_th_c': None, 'summer_fr_c': None, 'summer_sa_c': None, 'summer_su_c': None,
             'lunch_break_start': None, 'lunch_break_stop': None, 'processed': '24/7'},
            {'nonstop': None, 'mo_o': None, 'tu_o': None, 'we_o': None, 'th_o': None, 'fr_o': None, 'sa_o': None,
             'su_o': None, 'mo_c': None, 'tu_c': None, 'we_c': None, 'th_c': None, 'fr_c': None, 'sa_c': None,
             'su_c': None, 'summer_mo_o': None, 'summer_tu_o': None, 'summer_we_o': None, 'summer_th_o': None,
             'summer_fr_o': None, 'summer_sa_o': None, 'summer_su_o': None, 'summer_mo_c': None, 'summer_tu_c': None,
             'summer_we_c': None, 'summer_th_c': None, 'summer_fr_c': None, 'summer_sa_c': None, 'summer_su_c': None,
             'lunch_break_start': None, 'lunch_break_stop': None, 'processed': None},
            {'nonstop': False, 'mo_o': None, 'tu_o': None, 'we_o': None, 'th_o': None, 'fr_o': None, 'sa_o': None,
             'su_o': None, 'mo_c': None, 'tu_c': None, 'we_c': None, 'th_c': None, 'fr_c': None, 'sa_c': None,
             'su_c': None, 'summer_mo_o': None, 'summer_tu_o': None, 'summer_we_o': None, 'summer_th_o': None,
             'summer_fr_o': None, 'summer_sa_o': None, 'summer_su_o': None, 'summer_mo_c': None, 'summer_tu_c': None,
             'summer_we_c': None, 'summer_th_c': None, 'summer_fr_c': None, 'summer_sa_c': None, 'summer_su_c': None,
             'lunch_break_start': None, 'lunch_break_stop': None, 'processed': None},
            {'nonstop': None, 'mo_o': '08:00', 'tu_o': '10:00', 'we_o': '11:00', 'th_o': '08:00', 'fr_o': '08:00', 'sa_o': '11:20',
             'su_o': None, 'mo_c': '16:00', 'tu_c': '18:00', 'we_c': '14:00', 'th_c': '18:00', 'fr_c': '14:00', 'sa_c': '14:00',
             'su_c': None, 'summer_mo_o': None, 'summer_tu_o': None, 'summer_we_o': None, 'summer_th_o': None,
             'summer_fr_o': None, 'summer_sa_o': None, 'summer_su_o': None, 'summer_mo_c': None, 'summer_tu_c': None,
             'summer_we_c': None, 'summer_th_c': None, 'summer_fr_c': None, 'summer_sa_c': None, 'summer_su_c': None,
             'lunch_break_start': None, 'lunch_break_stop': None, 'processed': 'Mo 08:00-16:00;Tu 08:00-18:00;We 11:00-14:00;Th 10:00-18:00;Fr 08:00-14:00;Sa 11:20-14:00'},
            {'nonstop': None, 'mo_o': '08:00', 'tu_o': '08:00', 'we_o': '08:00', 'th_o': '10:00', 'fr_o': '10:00', 'sa_o': '11:20',
             'su_o': '11:20', 'mo_c': '16:00', 'tu_c': '16:00', 'we_c': '16:00', 'th_c': '18:00', 'fr_c': '14:00', 'sa_c': '14:00',
             'su_c': '14:00', 'summer_mo_o': None, 'summer_tu_o': None, 'summer_we_o': None, 'summer_th_o': None,
             'summer_fr_o': None, 'summer_sa_o': None, 'summer_su_o': None, 'summer_mo_c': None, 'summer_tu_c': None,
             'summer_we_c': None, 'summer_th_c': None, 'summer_fr_c': None, 'summer_sa_c': None, 'summer_su_c': None,
             'lunch_break_start': None, 'lunch_break_stop': None, 'processed': 'Mo,Th,We 08:00-16:00;Tu 10:00-18:00;Fr 10:00-14:00;Sa-Su 11:20-14:00'},
            {'nonstop': None, 'mo_o': '08:00', 'tu_o': '10:00', 'we_o': '08:00', 'th_o': '10:00', 'fr_o': '10:00', 'sa_o': '11:20',
             'su_o': '08:00', 'mo_c': '16:00', 'tu_c': '16:00', 'we_c': '16:00', 'th_c': '16:00', 'fr_c': '16:00', 'sa_c': '16:00',
             'su_c': '16:00', 'summer_mo_o': None, 'summer_tu_o': None, 'summer_we_o': None, 'summer_th_o': None,
             'summer_fr_o': None, 'summer_sa_o': None, 'summer_su_o': None, 'summer_mo_c': None, 'summer_tu_c': None,
             'summer_we_c': None, 'summer_th_c': None, 'summer_fr_c': None, 'summer_sa_c': None, 'summer_su_c': None,
             'lunch_break_start': None, 'lunch_break_stop': None, 'processed': 'Mo,Su,We 08:00-16:00;Fr,Th,Tu 10:00-16:00;Sa 11:20-16:00'},
            {'nonstop': None, 'mo_o': '08:00', 'tu_o': '09:00', 'we_o': '09:00', 'th_o': '10:00', 'fr_o': '10:00', 'sa_o': '11:20',
             'su_o': '08:00', 'mo_c': '16:00', 'tu_c': '16:00', 'we_c': '16:00', 'th_c': '16:00', 'fr_c': '16:00', 'sa_c': '16:00',
             'su_c': '16:00', 'summer_mo_o': None, 'summer_tu_o': None, 'summer_we_o': None, 'summer_th_o': None,
             'summer_fr_o': None, 'summer_sa_o': None, 'summer_su_o': None, 'summer_mo_c': None, 'summer_tu_c': None,
             'summer_we_c': None, 'summer_th_c': None, 'summer_fr_c': None, 'summer_sa_c': None, 'summer_su_c': None,
             'lunch_break_start': None, 'lunch_break_stop': None, 'processed': 'Mo,Su 08:00-16:00;Fr,Tu 10:00-16:00;We-Th 09:00-16:00;Sa 11:20-16:00'},
            {'nonstop': None, 'mo_o': '08:00', 'tu_o': '09:00', 'we_o': '09:00', 'th_o': '10:00', 'fr_o': '10:00',
             'sa_o': '11:20',
             'su_o': '08:00', 'mo_c': '16:00', 'tu_c': '16:00', 'we_c': '16:00', 'th_c': '16:00', 'fr_c': '16:00',
             'sa_c': '16:00',
             'su_c': '16:00', 'summer_mo_o': None, 'summer_tu_o': None, 'summer_we_o': None, 'summer_th_o': None,
             'summer_fr_o': None, 'summer_sa_o': None, 'summer_su_o': None, 'summer_mo_c': None, 'summer_tu_c': None,
             'summer_we_c': None, 'summer_th_c': None, 'summer_fr_c': None, 'summer_sa_c': None, 'summer_su_c': None,
             'lunch_break_start': '12:00', 'lunch_break_stop': '12:30',
             'processed': 'Mo,Su 08:00-12:00,12:30-16:00;Fr,Tu 10:00-12:00,12:30-16:00;We-Th 09:00-12:00,12:30-16:00;Sa 11:20-12:00,12:30-16:00'},
        ]

    def test_extract_opening_hours(self):
        for i in self.opening_hours:
            p = OpeningHours(i['nonstop'], i['mo_o'], i['th_o'], i['we_o'], i['tu_o'], i['fr_o'], i['sa_o'],
                                      i['su_o'], i['mo_c'], i['th_c'], i['we_c'], i['tu_c'], i['fr_c'], i['sa_c'],
                                      i['su_c'], i['summer_mo_o'], i['summer_th_o'], i['summer_we_o'], i['summer_tu_o'],
                                      i['summer_fr_o'], i['summer_sa_o'], i['summer_su_o'], i['summer_mo_c'],
                                      i['summer_th_c'], i['summer_we_c'], i['summer_tu_c'], i['summer_fr_c'],
                                      i['summer_sa_c'], i['summer_su_c'], i['lunch_break_start'], i['lunch_break_stop'])
            with self.subTest():
                self.assertEqual(i['processed'], p.process())
