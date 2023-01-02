# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    import time
    from osm_poi_matchmaker.utils.timing import Timing
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestTiming(unittest.TestCase):
    def setUp(self):
        self.one_sec = '0:00:00.10'
        self.one_sec_timer = 0.1

    def test_timing(self):
        timer = Timing()
        time.sleep(self.one_sec_timer)
        end = timer.end()
        self.assertRegex(end, '{}.*'.format(self.one_sec), 10)
