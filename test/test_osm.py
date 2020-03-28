# -*- coding: utf-8 -*-

try:
    import unittest
    import traceback
    import logging
    import sys
    from osm_poi_matchmaker.libs.osm import relationer
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


class TestOSMRelationer(unittest.TestCase):
    def setUp(self):
        self.test_data = [
            {'original': ['w25291279', 'outer', 'w25291280', 'inner'],
             'output': [{'ref': '25291279', 'role': 'outer', 'type': 'way'},
                        {'ref': '25291280', 'role': 'inner', 'type': 'way'}]},
            {'original': ['r555643', '', 'r555642', ''],
             'output': [{'ref': '555643', 'role': '', 'type': 'relation'},
                        {'ref': '555642', 'role': '', 'type': 'relation'}]},
        ]

    def test_relationer(self):
        for i in self.test_data:
            original, output = i['original'], i['output']
            a = relationer(original)
            with self.subTest():
                self.assertListEqual(output, a)
