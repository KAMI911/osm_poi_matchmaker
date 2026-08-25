# -*- coding: utf-8 -*-
__author__ = 'kami911'

try:
    import logging
    import sys
    import enum
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

__program__ = 'enums'
__version__ = '0.0.1'


class WeekDaysShort(enum.Enum):
    """Monday=0..Sunday=6, keyed by OSM-style two-letter day abbreviations."""
    mo = 0
    tu = 1
    we = 2
    th = 3
    fr = 4
    sa = 5
    su = 6


class WeekDaysLong(enum.Enum):
    """Monday=0..Sunday=6, keyed by full English day names."""
    Monday = 0
    Tuesday = 1
    Wednesday = 2
    Thursday = 3
    Friday = 4
    Saturday = 5
    Sunday = 6


class OpenClose(enum.Enum):
    """Row labels used to index a per-day opening-hours table (see poi_dataset.py)."""
    open = 0
    close = 1
    summer_open = 2
    summer_close = 3


class WeekDaysLongHU(enum.Enum):
    """Monday=0..Sunday=6, keyed by full Hungarian day names.

    'Csütörtökön' (inflected form of 'Csütörtök', Thursday) is included as a second
    alias for 3 because at least one data source uses that form instead."""
    Hétfő = 0
    Kedd = 1
    Szerda = 2
    Csütörtök = 3
    Csütörtökön = 3
    Péntek = 4
    Szombat = 5
    Vasárnap = 6


class WeekDaysLongHUUnAccented(enum.Enum):
    """Monday=0..Sunday=6, keyed by full Hungarian day names without accents, for
    data sources that strip diacritics."""
    Hetfo = 0
    Kedd = 1
    Szerda = 2
    Csutortok = 3
    Pentek = 4
    Szombat = 5
    Vasarnap = 6


class FileType(enum.Enum):
    """Source file formats supported by libs/soup.py's download/cache helpers."""
    html = 0
    json = 1
    csv = 2
    xml = 3
    zip = 4
