#!/usr/bin/python
# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    from test.test_address import TestAddressResolver, TestFullAddressResolver, TestOpeningHoursCleaner, \
        TestOpeningHoursCleaner2, TestPhoneClener, TestPhoneClener_to_str, TestStringCleaner, TestURLCleaner, \
        TestCityCleaner, TestReplaceHTMLNewLines, TestExtractPhoneNumber
    from test.test_online_poi_matching import TestSmartOnlinePOIMatching
    from test.test_opening_hours import TestOpeningHours
    # from test.test_poi_dataset import TestPOIDatasetRaw
    from test.test_timing import TestTiming
    from test.test_osm import TestOSMRelationer
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def testing_create_db():
    address_resolver = unittest.TestLoader().loadTestsFromTestCase(TestAddressResolver)
    address_full_resolver = unittest.TestLoader().loadTestsFromTestCase(TestFullAddressResolver)
    opening_hours_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestOpeningHoursCleaner)
    opening_hours_cleaner2 = unittest.TestLoader().loadTestsFromTestCase(TestOpeningHoursCleaner2)
    city_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestCityCleaner)
    replace_html_newlines = unittest.TestLoader().loadTestsFromTestCase(TestReplaceHTMLNewLines)
    extract_phone_number = unittest.TestLoader().loadTestsFromTestCase(TestExtractPhoneNumber)
    phone_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestPhoneClener)
    phone_cleaner_to_str = unittest.TestLoader().loadTestsFromTestCase(TestPhoneClener_to_str)
    string_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestStringCleaner)
    url_cleaner = unittest.TestLoader().loadTestsFromTestCase(TestURLCleaner)
    opening_hours_resolver = unittest.TestLoader().loadTestsFromTestCase(TestOpeningHours)
    smart_online_poi_matching = unittest.TestLoader().loadTestsFromTestCase(TestSmartOnlinePOIMatching)
    # poi_dataset = unittest.TestLoader().loadTestsFromTestCase(TestPOIDatasetRaw)
    timing = unittest.TestLoader().loadTestsFromTestCase(TestTiming)
    osm = unittest.TestLoader().loadTestsFromTestCase(TestOSMRelationer)
    suite = unittest.TestSuite(
        [address_resolver, address_full_resolver, opening_hours_cleaner, opening_hours_cleaner2, city_cleaner,
         replace_html_newlines, extract_phone_number, phone_cleaner, phone_cleaner_to_str, string_cleaner, url_cleaner, opening_hours_resolver,
         smart_online_poi_matching, timing, osm])
    return unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    testing_create_db()
