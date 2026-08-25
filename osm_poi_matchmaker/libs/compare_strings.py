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
    """Classify a proposed tag value against the existing OSM value, for the
    human-readable diff comment file_output.py writes into the generated OSM XML.

    Args:
        string1: The new (proposed) value.
        string2: The existing OSM value (default '').

    Returns:
        str: ' NEW ' if string1 is set but string2 wasn't, ' DEL ' if string2 was set
        but string1 isn't, ' MOD ' if both are set but differ, or ' EQU ' if equal.
    """
    # New string
    if (string1 == '' or string1 is None) and (string2 != '' and string2 is not None):
        return ' NEW '
    # Deleted string
    elif (string1 != '' and string1 is not None) and (string2 == '' or string2 is None):
        return ' DEL '
    # Modified string
    elif str(string1) != str(string2):
        return ' MOD '
    # Equal string
    elif str(string1) == str(string2):
        return ' EQU '
