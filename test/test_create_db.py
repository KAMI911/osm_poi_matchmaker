#!/usr/bin/python
# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    from test.test_address import TestAddressResolver, TestFullAddressResolver, TestOpeningHoursCleaner, \
        TestOpeningHoursCleaner2, TestPhoneCleaner, TestPhoneCleanerStr, TestPhoneCleanerMobileStr, \
        TestStringCleaner, TestURLCleaner, TestCityCleaner, TestPostcodeCleaner, TestReplaceHTMLNewLines, \
        TestExtractPhoneNumber
    from test.test_online_poi_matching import TestSmartOnlinePOIMatching
    from test.test_opening_hours import TestOpeningHours
    from test.test_file_output_helper import TestURLTagGenerator
    # from test.test_poi_dataset import TestPOIDatasetRaw
    from test.test_timing import TestTiming
    from test.test_osm import TestOSMRelationer
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


if __name__ == '__main__':
    config.set_mode(config.Mode.matcher)
    suite = unittest.TestSuite([
        unittest.TestLoader().loadTestsFromTestCase(TestAddressResolver),
        unittest.TestLoader().loadTestsFromTestCase(TestFullAddressResolver),
        unittest.TestLoader().loadTestsFromTestCase(TestOpeningHoursCleaner),
        unittest.TestLoader().loadTestsFromTestCase(TestOpeningHoursCleaner2),
        unittest.TestLoader().loadTestsFromTestCase(TestCityCleaner),
        unittest.TestLoader().loadTestsFromTestCase(TestPostcodeCleaner),
        unittest.TestLoader().loadTestsFromTestCase(TestReplaceHTMLNewLines),
        unittest.TestLoader().loadTestsFromTestCase(TestExtractPhoneNumber),
        unittest.TestLoader().loadTestsFromTestCase(TestPhoneCleaner),
        unittest.TestLoader().loadTestsFromTestCase(TestPhoneCleanerStr),
        unittest.TestLoader().loadTestsFromTestCase(TestPhoneCleanerMobileStr),
        unittest.TestLoader().loadTestsFromTestCase(TestStringCleaner),
        unittest.TestLoader().loadTestsFromTestCase(TestURLCleaner),
        unittest.TestLoader().loadTestsFromTestCase(TestOpeningHours),
        unittest.TestLoader().loadTestsFromTestCase(TestSmartOnlinePOIMatching),
        unittest.TestLoader().loadTestsFromTestCase(TestURLTagGenerator),
        unittest.TestLoader().loadTestsFromTestCase(TestTiming),
        unittest.TestLoader().loadTestsFromTestCase(TestOSMRelationer),
    ])

    #runner = unittest.TextTestRunner(verbosity=1, stream=sys.stdout, descriptions=True)  # verbose output
    runner = unittest.TextTestRunner(verbosity=2, descriptions=True)  # verbose output
    runner.run(suite)