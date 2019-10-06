#!/usr/bin/python
# -*- coding: utf-8 -*-

try:
    import unittest
    from test.test_address import TestAddressResolver, TestFullAddressResolver, TestOpeningHoursClener, TestOpeningHoursClener2, \
        TestPhoneClener, TestPhoneClener_to_str, TestStringCleaner, TestURLCleaner
    from test.test_opening_hours import TestOpeningHours
    from test.test_poi_dataset import TestPOIDataset
    from test.test_timing import Timing
    from test.test_osm import TestOSMRelationer
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


def testing_create_db():
    address_resolver = unittest.TestLoader().loadTestsFromTestCase(TestAddressResolver)
    address_full_resolver = unittest.TestLoader().loadTestsFromTestCase(TestFullAddressResolver)
    opening_hours_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestOpeningHoursClener)
    opening_hours_cleaner2 = unittest.TestLoader().loadTestsFromTestCase(TestOpeningHoursClener2)
    phone_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestPhoneClener)
    phone_cleaner_to_str = unittest.TestLoader().loadTestsFromTestCase(TestPhoneClener_to_str)
    string_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestStringCleaner)
    url_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestURLCleaner)
    opening_hours_resolver = unittest.TestLoader().loadTestsFromTestCase(TestOpeningHours)
    poi_dataset = unittest.TestLoader().loadTestsFromTestCase(TestPOIDataset)
    timing = unittest.TestLoader().loadTestsFromTestCase(Timing)
    osm = unittest.TestLoader().loadTestsFromTestCase(TestOSMRelationer)
    suite = unittest.TestSuite(
        [address_resolver, address_full_resolver, opening_hours_cleaner, opening_hours_cleaner2, phone_cleaner, phone_cleaner_to_str,
         string_cleaner, url_cleaner, opening_hours_resolver, poi_dataset, timing, osm])
    return unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    testing_create_db()
