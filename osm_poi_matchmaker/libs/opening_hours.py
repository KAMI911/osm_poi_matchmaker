# -*- coding: utf-8 -*-

try:
    import logging
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


def extract_opening_hours(nonstop, mo_o, th_o, we_o, tu_o, fr_o, sa_o, su_o, mo_c, th_c, we_c, tu_c,
                          fr_c, sa_c, su_c, summer_mo_o, summer_th_o, summer_we_o, summer_tu_o, summer_fr_o,
                          summer_sa_o, summer_su_o, summer_mo_c, summer_th_c, summer_we_c, summer_tu_c,
                          summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunck_break_stop):
    if nonstop == True:
        return '24/7'
    return None
