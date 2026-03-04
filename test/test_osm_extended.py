# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    import re
    import datetime
    from osm_poi_matchmaker.libs.osm import relationer, timestamp_now, osm_timestamp_now
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestOSMRelationerEdgeCases(unittest.TestCase):
    def setUp(self):
        self.test_data = [
            {
                'original': None,
                'output': None,
            },
            {
                'original': [],
                'output': [],
            },
            {
                'original': ['n12345678', 'outer'],
                'output': [{'ref': '12345678', 'role': 'outer', 'type': 'node'}],
            },
            {
                'original': ['w25291279', 'outer', 'n12345', ''],
                'output': [
                    {'ref': '25291279', 'role': 'outer', 'type': 'way'},
                    {'ref': '12345', 'role': '', 'type': 'node'},
                ],
            },
            {
                'original': ['x99999', ''],
                'output': [{'ref': '99999', 'role': '', 'type': 'unknown'}],
            },
        ]

    def test_relationer_edge_cases(self):
        for i in self.test_data:
            original, output = i['original'], i['output']
            with self.subTest(original=original):
                result = relationer(original)
                self.assertEqual(output, result)


class TestOSMTimestamp(unittest.TestCase):
    def test_timestamp_now_returns_datetime(self):
        result = timestamp_now()
        self.assertIsInstance(result, datetime.datetime)

    def test_timestamp_now_is_recent(self):
        before = datetime.datetime.now()
        result = timestamp_now()
        after = datetime.datetime.now()
        self.assertGreaterEqual(result, before)
        self.assertLessEqual(result, after)

    def test_osm_timestamp_now_returns_string(self):
        result = osm_timestamp_now()
        self.assertIsInstance(result, str)

    def test_osm_timestamp_now_format(self):
        result = osm_timestamp_now()
        pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')
        self.assertRegex(result, pattern)
