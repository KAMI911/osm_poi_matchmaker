# -*- coding: utf-8 -*-

try:
    import re
    import logging
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)

def compare_strings(string1, string2 = ''):
    if (string1 is '' or string1 is None) and (string2 is not '' and string2 is not None):
        return 'N'
    elif (string1 is not '' and string1 is not None) and (string2 is '' or string2 is None):
        return 'D'
    elif  string1 != string2:
        return 'M'
    elif string1 == string2:
        return 'E'