# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import re
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def compare_strings(string1, string2=''):
    # New string
    if (string1 is '' or string1 is None) and (string2 is not '' and string2 is not None):
        return ' NEW '
    # Deleted string
    elif (string1 is not '' and string1 is not None) and (string2 is '' or string2 is None):
        return ' DEL '
    # Modified string
    elif str(string1) != str(string2):
        return ' MOD '
    # Equal string
    elif str(string1) == str(string2):
        return ' EQU '
