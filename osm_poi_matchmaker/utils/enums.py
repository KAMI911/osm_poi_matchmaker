# -*- coding: utf-8 -*-
__author__ = 'kami911'

try:
    import traceback
    import logging
    import enum
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)

__program__ = 'enums'
__version__ = '0.0.1'


class WeekDaysShort(enum.Enum):
    mo = 0
    tu = 1
    we = 2
    th = 3
    fr = 4
    sa = 5
    su = 6


class WeekDaysLong(enum.Enum):
    Monday = 0
    Tuesday = 1
    Wednesday = 2
    Thursday = 3
    Friday = 4
    Saturday = 5
    Sunday = 6


class OpenClose(enum.Enum):
    open = 0
    close = 1
    summer_open = 2
    summer_close = 3


class WeekDaysLongHU(enum.Enum):
    Hétfő = 0
    Kedd = 1
    Szerda = 2
    Csütörtök = 3
    Csütörtökön = 3
    Péntek = 4
    Szombat = 5
    Vasárnap = 6


class WeekDaysLongHUUnAccented(enum.Enum):
    Hetfo = 0
    Kedd = 1
    Szerda = 2
    Csutortok = 3
    Pentek = 4
    Szombat = 5
    Vasarnap = 6
