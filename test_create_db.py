#!/usr/bin/python
# -*- coding: utf-8 -*-

try:
    import unittest
    from test.test_address import TestAddressResolver, TestFullAddressResolver, OpeningHoursClener, OpeningHoursClener2

except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


def testing_create_db():
    address_resolver = unittest.TestLoader().loadTestsFromTestCase(TestAddressResolver)
    address_full_resolver = unittest.TestLoader().loadTestsFromTestCase(TestFullAddressResolver)
    opening_hours_cleaner = unittest.TestLoader().loadTestsFromTestCase(OpeningHoursClener)
    opening_hours_cleaner2 = unittest.TestLoader().loadTestsFromTestCase(OpeningHoursClener2)
    suite = unittest.TestSuite(
        [address_resolver, address_full_resolver, opening_hours_cleaner, opening_hours_cleaner2])
    return unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__':
    testing_create_db()
