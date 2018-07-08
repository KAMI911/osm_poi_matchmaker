# -*- coding: utf-8 -*-
__author__ = 'kami911'

try:
    import traceback
    import enum
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

__program__ = 'enums'
__version__ = '0.0.1'


class WeekDaysLongHU(enum.Enum):
    Hétfő = 0
    Kedd = 1
    Szerda = 2
    Csütörtök = 3
    Péntek = 4
    Szombat = 5
    Vasárnap = 6
