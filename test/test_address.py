# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, extract_all_address, \
        clean_opening_hours, clean_opening_hours_2, clean_phone, clean_phone_to_str, clean_phone_and_mobile_to_str, \
        clean_string, clean_url, clean_city, replace_html_newlines, extract_phone_number, clean_postcode, \
        extract_all_address_waxeye
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestAddressResolver(unittest.TestCase):
    def setUp(self):
        self.addresses = [
            {'original': 'Gránátos u. 11.', 'street': 'Gránátos utca', 'housenumber': '11', 'conscriptionnumber': None},
            {'original': 'BERCSÉNYI U.1 2934/5 HRSZ', 'street': 'Bercsényi Miklós utca', 'housenumber': '1',
             'conscriptionnumber': '2934/5'},
            {'original': 'Szérűskert utca 018910/23. hrsz. (Köles utca 1.)', 'street': 'Szérűskert utca',
             'housenumber': None,
             'conscriptionnumber': '018910/23'},
            {'original': 'Palotai út 6. (Fehér Palota Üzletközpont)', 'street': 'Palotai út',
             'housenumber': '6', 'conscriptionnumber': None},
            {'original': 'Budai Vám 1.', 'street': 'Budai Vám',
             'housenumber': '1', 'conscriptionnumber': None},
            {'original': 'Kaszás u. 2.-Dózsa György út 87.', 'street': 'Dózsa György út',
             'housenumber': '87',
             'conscriptionnumber': None},
            {'original': 'Bajcsy Zs. út 11.', 'street': 'Bajcsy-Zsilinszky út', 'housenumber': '11',
             'conscriptionnumber': None},
            {'original': 'Hunyadi János út 19. - Savoya Park', 'street': 'Hunyadi János út', 'housenumber': '19',
             'conscriptionnumber': None},
            {'original': 'Kölcsey F. utca 1.', 'street': 'Kölcsey Ferenc utca', 'housenumber': '1',
             'conscriptionnumber': None},
            {'original': 'Várkerület 41.', 'street': None, 'housenumber': None,
             'conscriptionnumber': None},  # TODO: this is wrong
            {'original': 'Bajcsy-Zs. E. u. 31.', 'street': 'Bajcsy-Zsilinszky Endre utca', 'housenumber': '31',
             'conscriptionnumber': None},
            {'original': 'Bajcsy Zs.u. 77.', 'street': 'Bajcsy-Zsilinszky utca', 'housenumber': '77',
             'conscriptionnumber': None},
            {'original': 'Dózsa Gy.u.6.', 'street': 'Dózsa György utca', 'housenumber': '6',
             'conscriptionnumber': None},
            {'original': 'Krisztina krt. 65-67.', 'street': 'Krisztina körút', 'housenumber': '65-67',
             'conscriptionnumber': None},
            {'original': 'OLADI LTP. (DOLGOZÓK U.)', 'street': 'OLADI lakótelep', 'housenumber': None,
             'conscriptionnumber': None},
            {'original': 'Fő út 24.', 'street': 'Fő út', 'housenumber': '24',
             'conscriptionnumber': None},
            {'original': 'Törvényház u. 4.', 'street': 'Törvényház utca', 'housenumber': '4',
             'conscriptionnumber': None},
            {'original': 'Alkotás u. 53.', 'street': 'Alkotás utca', 'housenumber': '53',
             'conscriptionnumber': None},
            {'original': 'Szilas pihenő M3', 'street': 'Szilas pihenő', 'housenumber': '3',
             'conscriptionnumber': None}, #TODO Fix this
            {'original': 'Üllői út 129', 'street': 'Üllői út', 'housenumber': '129',
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
             'street': None, 'housenumber': None, 'conscriptionnumber': None},
            {'original': '2463 Tordas Köztársaság u.8/a.', 'postcode': '2463', 'city': 'Tordas',
             'street': 'Köztársaság utca', 'housenumber': '8/a', 'conscriptionnumber': None},
            {'original': '2000 Szentendre Vasvári Pál u. 2794/16 hrsz.', 'postcode': '2000', 'city': 'Szentendre',
             'street': 'Vasvári Pál utca', 'housenumber': None, 'conscriptionnumber': '2794/16'},
            {'original': '1213 Budapest, XXI, 21 Juharos utca 34.', 'postcode': '1213', 'city': 'Budapest',
             'street': 'Juharos utca', 'housenumber': '34', 'conscriptionnumber': None},
            {'original': '1011 Budapest, I, 01 Batthyány tér 5.', 'postcode': '1011', 'city': 'Budapest',
             'street': 'Batthyány tér', 'housenumber': '5', 'conscriptionnumber': None},
        ]

    '''
    def test_extract_all_address(self):
        for i in self.addresses:
            original, postcode, city, street, housenumber, conscriptionnumber = i['original'], i['postcode'], \
                                                                                i['city'], i['street'], \
                                                                                i['housenumber'], \
                                                                                i['conscriptionnumber']
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
    '''

    def test_extract_all_address_waxeye(self):
        for i in self.addresses:
            original, postcode, city, street, housenumber, conscriptionnumber = i['original'], i['postcode'], \
                                                                                i['city'], i['street'], \
                                                                                i['housenumber'], \
                                                                                i['conscriptionnumber']
            check_postcode, check_city, check_street, check_housenumber, \
                check_conscriptionnumber = extract_all_address_waxeye(original)
            with self.subTest():
                self.assertEqual(postcode, check_postcode)
            with self.subTest():
                self.assertEqual(city, check_city)
            with self.subTest():
                self.assertEqual(street, check_street)
            with self.subTest():
                self.assertEqual(housenumber, check_housenumber)
            with self.subTest():
                self.assertEqual(conscriptionnumber, check_conscriptionnumber)


class TestOpeningHoursCleaner(unittest.TestCase):
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


class TestOpeningHoursCleaner2(unittest.TestCase):
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


class TestPhoneCleaner(unittest.TestCase):
    def setUp(self):
        self.phones = [
            {'original': '0684/330-734, 0630/2374-712', 'converted': ['+36 84 330 734', '+36 30 237 4712']},
            {'original': '06-20-200-4000', 'converted': ['+36 20 200 4000']},
            {'original': '62464024', 'converted': ['+36 62 464 024']},
            {'original': ' 3684330 - 734', 'converted': ['+36 84 330 734']},
            {'original': '06205089009(Központi Telszám: Benzinkút, Motel, Kávézó, Szobafoglalás)',
             'converted': ['+36 20 508 9009']},
            {'original': '  ', 'converted': None},
            {'original': '+36 20 2976 393', 'converted': ['+36 20 297 6393']},

        ]

    def test_clean_phone(self):
        for i in self.phones:
            original, ph = i['original'], i['converted']
            a = clean_phone(original)
            with self.subTest():
                self.assertEqual(ph, a)


class TestPhoneCleanerStr(unittest.TestCase):
    def setUp(self):
        self.phones = [
            {'original': '0684/330-734, 0630/2374-712', 'converted': '+36 84 330 734;+36 30 237 4712'},
            {'original': '06-20-200-4000', 'converted': '+36 20 200 4000'},
            {'original': '62464024', 'converted': '+36 62 464 024'},
            {'original': ' 3684330 - 734', 'converted': '+36 84 330 734'},
            {'original': '06205089009(Központi Telszám: Benzinkút, Motel, Kávézó, Szobafoglalás)',
             'converted': '+36 20 508 9009'},
            {'original': '  ', 'converted': None},
            {'original': '+36303035698', 'converted': '+36 30 303 5698'},
        ]

    def test_clean_phone(self):
        for i in self.phones:
            original, ph = i['original'], i['converted']
            a = clean_phone_to_str(original)
            with self.subTest():
                self.assertEqual(ph, a)


class TestPhoneCleanerMobileStr(unittest.TestCase):
    def setUp(self):
        self.phones = [
            {'original': '0684/330-734, 0630/2374-712', 'converted_phone': '+36 84 330 734', 'converted_mobile': '+36 30 237 4712'},
            {'original': '06-20-200-4000', 'converted_phone': None, 'converted_mobile': '+36 20 200 4000'},
            {'original': '62464024', 'converted_phone': '+36 62 464 024', 'converted_mobile': None},
            {'original': ' 3684330 - 734', 'converted_phone': '+36 84 330 734', 'converted_mobile': None},
            {'original': '06205089009(Központi Telszám: Benzinkút, Motel, Kávézó, Szobafoglalás)', 'converted_phone': None,
             'converted_mobile': '+36 20 508 9009'},
            {'original': '  ', 'converted_phone': None, 'converted_mobile': None},
            {'original': '+36303035698', 'converted_phone': None, 'converted_mobile': '+36 30 303 5698'},
        ]

    def test_clean_phone(self):
        for i in self.phones:
            original, ph, mo = i['original'], i['converted_phone'], i['converted_mobile']
            a, b = clean_phone_and_mobile_to_str(original)
            with self.subTest():
                self.assertEqual(ph, a)
                self.assertEqual(mo, b)

class TestStringCleaner(unittest.TestCase):
    def setUp(self):
        self.phones = [
            {'original': '  ablak  zsiráf   ', 'converted': 'ablak zsiráf'},
            {'original': None, 'converted': None},
            {'original': '    ', 'converted': None},
            {'original': 'None', 'converted': None},
            {'original': '   None   ', 'converted': None},
            {'original': ' NULL', 'converted': None},
            {'original': ' NULLNone', 'converted': None},
            {'original': ' \n\nNULLNone', 'converted': None},
            {'original': '\n', 'converted': None},
            {'original': '\t', 'converted': None},
        ]

    def test_clean_string(self):
        case = 0
        for i in self.phones:
            case += 1
            original, ph = i['original'], i['converted']
            a = clean_string(original)
            with self.subTest():
                self.assertEqual(ph, a, 'Case {}'.format(case))


class TestURLCleaner(unittest.TestCase):
    def setUp(self):
        self.urls = [
            {'original': '  https://examle.com//tests//url//   ', 'converted': 'https://examle.com/tests/url/'},
            {'original': '  https://examle.com/////tests///url     ', 'converted': 'https://examle.com/tests/url'},
        ]

    def test_clean_url(self):
        for i in self.urls:
            original, ph = i['original'], i['converted']
            a = clean_url(original)
            with self.subTest():
                self.assertEqual(ph, a)


class TestCityCleaner(unittest.TestCase):
    def setUp(self):
        self.addresses = [
            {'original': 'Bük', 'city': 'Bük'},
            {'original': 'Csanádapáca', 'city': 'Csanádapáca'},
            {'original': 'Tordas', 'city': 'Tordas'},
            {'original': 'Szentendre', 'city': 'Szentendre'},
            {'original': 'Budapest I. Kerület', 'city': 'Budapest'},
            {'original': 'Budapest Xxiii. Kerület', 'city': 'Budapest'},
            {'original': 'Mikolc', 'city': 'Miskolc'},
            {'original': 'Iinárcs', 'city': 'Inárcs'},
        ]

    def test_clean_city(self):
        for i in self.addresses:
            original, city = i['original'], i['city']
            a = clean_city(original)
            with self.subTest():
                self.assertEqual(city, a)


class TestPostcodeCleaner(unittest.TestCase):
    def setUp(self):
        self.addresses = [
            {'original': '1111', 'postcode': '1111'},
            {'original': '    1111     ', 'postcode': '1111'},
            {'original': '    1111', 'postcode': '1111'},
            {'original': '1111    ', 'postcode': '1111'},
            {'original': '', 'postcode': None},
            {'original': ' ', 'postcode': None},
            {'original': '0', 'postcode': None},
            {'original': 'None', 'postcode': None},
        ]

    def test_clean_city(self):
        for i in self.addresses:
            original, postcode = i['original'], i['postcode']
            a = clean_postcode(original)
            with self.subTest():
                self.assertEqual(postcode, a)


class TestReplaceHTMLNewLines(unittest.TestCase):
    def setUp(self):
        self.text = [
            {'original': 'Akadálymentesen megközelíthető fiók és ATM  <br> Telefonszám:(26) 501-400 </br>',
             'replaced': 'Akadálymentesen megközelíthető fiók és ATM; Telefonszám:(26) 501-400;'},
            {'original': 'Akadálymentesen megközelíthető fiók és ATM<br />A fiókban a Prémium szolgáltatás elérhető.  <br> Telefonszám:(22) 515-260 </br>',
             'replaced': 'Akadálymentesen megközelíthető fiók és ATM; A fiókban a Prémium szolgáltatás elérhető.; Telefonszám:(22) 515-260;'},
        ]

    def test_replace_html_newlines(self):
        for i in self.text:
            original, description = i['original'], i['replaced']
            a = replace_html_newlines(original)
            with self.subTest():
                self.assertEqual(description, a)

class TestExtractPhoneNumber(unittest.TestCase):
    def setUp(self):
        self.text = [
            {'original': 'Akadálymentesen megközelíthető fiók és ATM  <br> Telefonszám:(26) 501-400 </br>',
             'replaced': '+36 26 501 400'},
            {'original': 'Akadálymentesen megközelíthető fiók és ATM<br />A fiókban a Prémium szolgáltatás elérhető.  <br> Telefonszám:(22) 515-260 </br>',
             'replaced': '+36 22 515 260'},
            {'original': 'Pláza, földszint, Búza tér felőli bejárat<br />Akadálymentesen megközelíthető fiók és ATM  <br> Telefonszám:(46) 504-540 </br>',
             'replaced': '+36 46 504 540'},
        ]

    def test_extract_phone_number(self):
        for i in self.text:
            original, phone_number = i['original'], i['replaced']
            a = extract_phone_number(original)
            with self.subTest():
                self.assertEqual(phone_number, a)
