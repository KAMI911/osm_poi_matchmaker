# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import unittest
    from osm_poi_matchmaker.libs.file_output_helper import url_tag_generator
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
            {'poi_url_base': 'https://www.mkb.hu', 'poi_website': 'https://lny.io/MKB-WEB-HEREND', 'converted': 'https://lny.io/MKB-WEB-HEREND'},
        ]

    def test_url_tag_generator(self):
        for i in self.url:
            poi_url_base, poi_website, url = i['poi_url_base'], i['poi_website'], i['converted']
            a = url_tag_generator(poi_url_base, poi_website)
            with self.subTest():
                self.assertEqual(url, a)


if __name__ == '__main__':
    unittest.main()
