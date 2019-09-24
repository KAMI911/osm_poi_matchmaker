# -*- coding: utf-8 -*-

try:
    import unittest
    import time
    from osm_poi_matchmaker.utils.timing import Timing
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


class Timing(unittest.TestCase):
    def setUp(self):
        self.one_sec = '0:00:00.10'
        self.one_sec_timer = 0.1

    def test_timing(self):
        timer = Timing()
        time.sleep(self.one_sec_timer)
        end = timer.end()
        self.assertRegexpMatches(end, '{}.*'.format(self.one_sec), 10)
