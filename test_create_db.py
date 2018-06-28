#!/usr/bin/python
# -*- coding: utf-8 -*-

try:
    import unittest
    from test.test_address import TestAddressResolver, TestFullAddressResolver, OpeningHoursClener, OpeningHoursClener2, \
        PhoneClener
    from test.test_opening_hours import TestOpeningHours
    from test.test_timing import Timing

except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


def testing_create_db():
    address_resolver = unittest.TestLoader().loadTestsFromTestCase(TestAddressResolver)
    address_full_resolver = unittest.TestLoader().loadTestsFromTestCase(TestFullAddressResolver)
    opening_hours_cleaner = unittest.TestLoader().loadTestsFromTestCase(OpeningHoursClener)
    opening_hours_cleaner2 = unittest.TestLoader().loadTestsFromTestCase(OpeningHoursClener2)
    phone_cleaner = unittest.TestLoader().loadTestsFromTestCase(PhoneClener)
    opening_hours_resolver = unittest.TestLoader().loadTestsFromTestCase(TestOpeningHours)
    timing = unittest.TestLoader().loadTestsFromTestCase(Timing)
    suite = unittest.TestSuite(
        [address_resolver, address_full_resolver, opening_hours_cleaner, opening_hours_cleaner2, phone_cleaner, opening_hours_resolver, timing])
    return unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__':
    testing_create_db()
