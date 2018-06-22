# -*- coding: utf-8 -*-

try:
    import unittest
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, extract_all_address, \
        clean_opening_hours, clean_opening_hours_2, clean_phone
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


class TestAddressResolver(unittest.TestCase):
    def setUp(self):
        self.addresses = [
            {'original': 'Gránátos u. 11.', 'street': 'Gránátos utca', 'housenumber': '11', 'conscriptionnumber': None},
            {'original': 'BERCSÉNYI U.1 2934/5 HRSZ', 'street': 'Bercsényi utca', 'housenumber': '1',
             'conscriptionnumber': '2934/5'},
            {'original': 'Szérűskert utca 018910/23. hrsz. (Köles utca 1.)', 'street': 'Szérűskert utca',
             'housenumber': None,
             'conscriptionnumber': '018910/23'},
            {'original': 'Palotai út 6. (Fehér Palota Üzletközpont)', 'street': 'Palotai út',
             'housenumber': '6', 'conscriptionnumber': None},
            {'original': 'Budai Vám 1.', 'street': 'Budai Vám',
             'housenumber': '1', 'conscriptionnumber': None},
            {'original': 'Kaszás u. 2.-Dózsa György út 87.', 'street': 'Kaszás utca 2.-Dózsa György út',
             'housenumber': '87',
             'conscriptionnumber': None},  # TODO: this is wrong
            {'original': 'Bajcsy Zs. út 11.', 'street': 'Bajcsy-Zsilinszky Endre út', 'housenumber': '11',
             'conscriptionnumber': None},
            {'original': 'Hunyadi János út 19. - Savoya Park', 'street': 'Hunyadi János út', 'housenumber': '19',
             'conscriptionnumber': None},
            {'original': 'Kölcsey F. utca 1.', 'street': 'Kölcsey Ferenc utca', 'housenumber': '1',
             'conscriptionnumber': None},
            {'original': 'Várkerület 41.', 'street': None, 'housenumber': None,
             'conscriptionnumber': None},  # TODO: this is wrong
            {'original': 'Bajcsy-Zs. E. u. 31.', 'street': 'Bajcsy-Zsilinszky Endre utca', 'housenumber': '31',
             'conscriptionnumber': None},
            {'original': 'Bajcsy Zs.u. 77.', 'street': 'Bajcsy-Zsilinszky Endre utca', 'housenumber': '77',
             'conscriptionnumber': None},
            {'original': 'Dózsa Gy.u.6.', 'street': 'Dózsa György utca', 'housenumber': '6',
             'conscriptionnumber': None},
        ]

    def test_extract_street_housenumber_better_2(self):
        for i in self.addresses:
            original, street, housenumber, conscriptionnumber = i['original'], i['street'], i['housenumber'], i[
                'conscriptionnumber']
            a, b, c = extract_street_housenumber_better_2(original)
            with self.subTest():
                self.assertEqual(street, a)
            with self.subTest():
                self.assertEqual(housenumber, b)
            with self.subTest():
                self.assertEqual(conscriptionnumber, c)


class TestFullAddressResolver(unittest.TestCase):
    def setUp(self):
        self.addresses = [
            {'original': '9737 Bük, Petőfi utca 63. Fszt. 1.', 'postcode': '9737', 'city': 'Bük',
             'street': 'Petőfi utca', 'housenumber': '63',
             'conscriptionnumber': None},
            {'original': '5662 Csanádapáca', 'postcode': '5662', 'city': 'Csanádapáca',
             'street': None, 'housenumber': None, 'conscriptionnumber': None}]

    def test_extract_all_address(self):
        for i in self.addresses:
            original, postcode, city, street, housenumber, conscriptionnumber = i['original'], i['postcode'], i['city'], \
                                                                                i['street'], i['housenumber'], i[
                                                                                    'conscriptionnumber']
            a, b, c, d, e = extract_all_address(original)
            with self.subTest():
                self.assertEqual(postcode, a)
            with self.subTest():
                self.assertEqual(city, b)
            with self.subTest():
                self.assertEqual(street, c)
            with self.subTest():
                self.assertEqual(housenumber, d)
            with self.subTest():
                self.assertEqual(conscriptionnumber, e)


class OpeningHoursClener(unittest.TestCase):
    def setUp(self):
        self.opening_hours = [
            {'original': '05:20-19:38', 'opening_hours_open': '05:20', 'opening_hours_close': '19:38'},
            {'original': '6:44-21:00', 'opening_hours_open': '06:44', 'opening_hours_close': '21:00'},
            {'original': '05:20-19:38 Reggel nyolctól bejárat az üzleten át', 'opening_hours_open': '05:20',
             'opening_hours_close': '19:38'},
            {'original': '   7:41 - 22:30    ', 'opening_hours_open': '07:41', 'opening_hours_close': '22:30'},
            {'original': '  05:30  -  23:00 ', 'opening_hours_open': '05:30', 'opening_hours_close': '23:00'},
            {'original': '  -       ', 'opening_hours_open': None, 'opening_hours_close': None}, ]

    def test_clean_opening_hours(self):
        for i in self.opening_hours:
            original, oho, ohc = i['original'], i['opening_hours_open'], i['opening_hours_close']
            a, b = clean_opening_hours(original)
            with self.subTest():
                self.assertEqual(oho, a)
            with self.subTest():
                self.assertEqual(ohc, b)


class OpeningHoursClener2(unittest.TestCase):
    def setUp(self):
        self.opening_hours = [
            {'original': '600', 'converted': '06:00'},
            {'original': '0644', 'converted': '06:44'},
            {'original': '2359', 'converted': '23:59'},
            {'original': '-1', 'converted': None},
        ]

    def test_clean_opening_hours(self):
        for i in self.opening_hours:
            original, oho = i['original'], i['converted']
            a = clean_opening_hours_2(original)
            with self.subTest():
                self.assertEqual(oho, a)


class PhoneClener(unittest.TestCase):
    def setUp(self):
        self.phones = [
            {'original': '0684/330-734, 0630/2374-712', 'converted': '3684330734'},
            {'original': '06-20-200-4000', 'converted': '36202004000'},
            {'original': '62464024', 'converted': '3662464024'},
            {'original': ' 3684330 - 734', 'converted': '3684330734'},
            {'original': '06205089009(Központi Telszám: Benzinkút, Motel, Kávézó, Szobafoglalás)',
             'converted': '36205089009'},
            {'original': '  ', 'converted': None},
        ]

    def test_clean_phone(self):
        for i in self.phones:
            original, ph = i['original'], i['converted']
            a = clean_phone(original)
            with self.subTest():
                self.assertEqual(ph, a)
