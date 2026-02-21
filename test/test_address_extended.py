# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    import json
    from osm_poi_matchmaker.libs.address import (
        remove_whitespace,
        clean_javascript_variable,
        clean_street,
        clean_street_type,
        clean_branch,
        clean_email,
        clean_phone_to_json,
        extract_street_housenumber,
        extract_city_street_housenumber_address,
    )
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestRemoveWhitespace(unittest.TestCase):
    def setUp(self):
        self.cases = [
            {'input': 'hello world', 'rpl': '', 'expected': 'helloworld'},
            {'input': 'hello world', 'rpl': ' ', 'expected': 'hello world'},
            {'input': 'hello\tworld', 'rpl': '', 'expected': 'helloworld'},
            {'input': 'hello\nworld', 'rpl': '-', 'expected': 'hello-world'},
            {'input': 'a  b  c', 'rpl': '', 'expected': 'abc'},
            {'input': 'a  b  c', 'rpl': ' ', 'expected': 'a b c'},
            {'input': 'nospaces', 'rpl': '', 'expected': 'nospaces'},
            {'input': '', 'rpl': '', 'expected': ''},
        ]

    def test_remove_whitespace(self):
        for i in self.cases:
            with self.subTest(input=i['input'], rpl=i['rpl']):
                self.assertEqual(i['expected'], remove_whitespace(i['input'], i['rpl']))


class TestCleanJavascriptVariable(unittest.TestCase):
    def setUp(self):
        self.cases = [
            {
                'clearable': 'var myData = {"key": "value"};',
                'removable': 'myData',
                'expected': '{"key": "value"}',
            },
            {
                'clearable': '  var  myData  =  [1,2,3] ;',
                'removable': 'myData',
                'expected': '[1,2,3]',
            },
            {
                'clearable': 'var items = "hello";',
                'removable': 'items',
                'expected': '"hello"',
            },
            {
                'clearable': 'var data = null',
                'removable': 'data',
                'expected': 'null',
            },
        ]

    def test_clean_javascript_variable(self):
        for i in self.cases:
            with self.subTest(clearable=i['clearable']):
                result = clean_javascript_variable(i['clearable'], i['removable'])
                self.assertEqual(i['expected'], result)


class TestCleanStreet(unittest.TestCase):
    def setUp(self):
        self.cases = [
            {'original': None, 'expected': None},
            {'original': 'Bajcsy-Zsilinszky u. 3.', 'expected': 'Bajcsy-Zsilinszky utca 3.'},
            {'original': 'Kossuth krt. 5.', 'expected': 'Kossuth körút 5.'},
            {'original': 'Petőfi ltp. B épület', 'expected': 'Petőfi lakótelep B épület'},
            {'original': 'Bajcsy Zs.u. 7.', 'expected': 'Bajcsy-Zsilinszky utca 7.'},
            {'original': 'Kossuth L. utca 1.', 'expected': 'Kossuth Lajos utca 1.'},
        ]

    def test_clean_street(self):
        for i in self.cases:
            with self.subTest(original=i['original']):
                self.assertEqual(i['expected'], clean_street(i['original']))


class TestCleanStreetType(unittest.TestCase):
    def setUp(self):
        self.cases = [
            {'original': None, 'expected': None},
            {'original': '', 'expected': None},
            {'original': 'u.', 'expected': 'utca'},
            {'original': 'utca', 'expected': 'utca'},
            {'original': 'fkl. út', 'expected': 'főközlekedési út'},
            {'original': 'főút', 'expected': 'főközlekedési út'},
            {'original': 'ltp.', 'expected': 'lakótelep'},
            {'original': 'pu.', 'expected': 'pályaudvar'},
            {'original': 'út.', 'expected': 'út'},
        ]

    def test_clean_street_type(self):
        for i in self.cases:
            with self.subTest(original=i['original']):
                self.assertEqual(i['expected'], clean_street_type(i['original']))


class TestCleanBranch(unittest.TestCase):
    def setUp(self):
        self.cases = [
            {'original': None, 'expected': None},
            {'original': '', 'expected': None},
            {'original': 'Fiók', 'expected': 'fiók'},
            {'original': 'Atm', 'expected': 'ATM'},
            {'original': 'Központi Fiók', 'expected': 'központi fiók'},
            {'original': 'Ügyfélszolgálat', 'expected': 'ügyfélszolgálat'},
            {'original': '5. Sz. Fiók', 'expected': '5. számú fiók'},
            {'original': 'Üzletház', 'expected': 'üzletház'},
            {'original': 'Bevásárlóközpont', 'expected': 'bevásárlóközpont'},
        ]

    def test_clean_branch(self):
        for i in self.cases:
            with self.subTest(original=i['original']):
                self.assertEqual(i['expected'], clean_branch(i['original']))


class TestCleanEmail(unittest.TestCase):
    def setUp(self):
        self.cases = [
            {'original': None, 'expected': None},
            {'original': '   ', 'expected': None},
            {'original': 'test@example.com', 'expected': 'test@example.com'},
            {'original': 'TEST@EXAMPLE.COM', 'expected': 'test@example.com'},
            {'original': 'a@b.com;c@d.com', 'expected': 'a@b.com;c@d.com'},
        ]

    def test_clean_email(self):
        for i in self.cases:
            with self.subTest(original=i['original']):
                self.assertEqual(i['expected'], clean_email(i['original']))


class TestCleanPhoneToJson(unittest.TestCase):
    def setUp(self):
        self.cases = [
            {'original': None, 'expected': None},
            {'original': '  ', 'expected': None},
            {'original': '06-20-200-4000', 'expected': json.dumps(['+36 20 200 4000'])},
            {'original': '0684/330-734', 'expected': json.dumps(['+36 84 330 734'])},
        ]

    def test_clean_phone_to_json(self):
        for i in self.cases:
            with self.subTest(original=i['original']):
                self.assertEqual(i['expected'], clean_phone_to_json(i['original']))


class TestExtractStreetHousenumber(unittest.TestCase):
    """Tests for the simpler (legacy) extract_street_housenumber function."""

    def setUp(self):
        self.cases = [
            {'original': 'Fő utca 1.', 'street': 'Fő utca', 'housenumber': '1'},
            {'original': 'Kossuth u. 3.', 'street': 'Kossuth utca', 'housenumber': '3'},
            {'original': 'Petőfi krt. 10.', 'street': 'Petőfi körút', 'housenumber': '10'},
            {'original': 'Rákóczi út 25/A', 'street': 'Rákóczi út', 'housenumber': '25/A'},
            {'original': 'Ady Endre utca 5/B', 'street': 'Ady Endre utca', 'housenumber': '5/B'},
        ]

    def test_extract_street_housenumber(self):
        for i in self.cases:
            with self.subTest(original=i['original']):
                street, housenumber = extract_street_housenumber(i['original'])
                self.assertEqual(i['street'], street)
                self.assertEqual(i['housenumber'], housenumber)


class TestExtractCityStreetHousenumber(unittest.TestCase):
    def setUp(self):
        self.cases = [
            {
                'original': None,
                'city': None, 'street': None, 'housenumber': None, 'conscriptionnumber': None,
            },
            {
                'original': '',
                'city': None, 'street': None, 'housenumber': None, 'conscriptionnumber': None,
            },
            {
                'original': 'Budapest, Fő utca 1.',
                'city': 'Budapest', 'street': 'Fő utca', 'housenumber': '1', 'conscriptionnumber': None,
            },
            {
                'original': 'Pécs',
                'city': 'Pécs', 'street': None, 'housenumber': None, 'conscriptionnumber': None,
            },
        ]

    def test_extract_city_street_housenumber(self):
        for i in self.cases:
            original = i['original']
            with self.subTest(original=original):
                result = extract_city_street_housenumber_address(original)
                # Access by index to handle the function's inconsistent return length
                self.assertEqual(i['city'], result[0])
                self.assertEqual(i['street'], result[1])
                self.assertEqual(i['housenumber'], result[2])
                self.assertEqual(i['conscriptionnumber'], result[3])
