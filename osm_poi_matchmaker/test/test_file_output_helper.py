# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import unittest
    from osm_poi_matchmaker.libs.file_output_helper import url_tag_generator, source_date_domain
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

class TestURLTagGenerator(unittest.TestCase):
    def setUp(self):
        self.url = [
            {'poi_url_base': 'https://www.mkb.hu', 'poi_website': '', 'converted': 'https://www.mkb.hu'},
            {'poi_url_base': 'https://www.mkb.hu', 'poi_website': None, 'converted': 'https://www.mkb.hu'},
            {'poi_url_base': '', 'poi_website': '', 'converted': None},
            {'poi_url_base': None, 'poi_website': '', 'converted': None},
            {'poi_url_base': '', 'poi_website': None, 'converted': None},
            {'poi_url_base': None, 'poi_website': None, 'converted': None},
            {'poi_url_base': 'https://www.mkb.hu', 'poi_website': 'https://lny.io/MKB-WEB-HEREND', 'converted': 'https://lny.io/mkb-web-herend'},
        ]

    def test_url_tag_generator(self):
        for i in self.url:
            poi_url_base, poi_website, url = i['poi_url_base'], i['poi_website'], i['converted']
            a = url_tag_generator(poi_url_base, poi_website)
            with self.subTest():
                self.assertEqual(url, a)


class TestSourceDateDomain(unittest.TestCase):
    def test_host_of_an_absolute_url(self):
        self.assertEqual('www.lidl.hu', source_date_domain('https://www.lidl.hu'))
        self.assertEqual('www.amplifon.com', source_date_domain(' https://www.amplifon.com/hu '))

    def test_missing_or_unusable_base_url_gives_none(self):
        for value in (None, '', '  ', float('nan'), 'www.lidl.hu', 'lidl'):
            self.assertIsNone(source_date_domain(value), value)


if __name__ == '__main__':
    unittest.main()
