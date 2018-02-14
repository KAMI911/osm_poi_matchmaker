# -*- coding: utf-8 -*-

try:
    import unittest
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better, extract_all_address
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
             'housenumber': '6', 'conscriptionnumber': None}]

    def test_extract_street_housenumber_better(self):
        for i in self.addresses:
            original, street, housenumber, conscriptionnumber = i['original'], i['street'], i['housenumber'], i[
                'conscriptionnumber']
            a, b, c = extract_street_housenumber_better(original)
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
            {'original': '5662 Csanádapáca','postcode': '5662', 'city': 'Csanádapáca',
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
