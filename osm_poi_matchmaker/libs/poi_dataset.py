# -*- coding: utf-8 -*-
__author__ = 'kami911'

try:
    import traceback
    import numpy as np
    import pandas as pd
    from osm_poi_matchmaker.utils.enums import WeekDaysShort, OpenClose, WeekDaysLongHU
    from osm_poi_matchmaker.libs.opening_hours import OpeningHours
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

__program__ = 'poi_dataset'
__version__ = '0.0.1'

DAYS = ['mo', 'tu', 'we', 'th', 'fr', 'sa', 'su']
OPENCLOSE = ['open', 'close', 'summer_open', 'summer_close']


class POIDataset:

    def __init__(self):
        self.clear_all()

    def clear_all(self):
        self.__code = None
        self.__postcode = None
        self.__city = None
        self.__name = None
        self.__branch = None
        self.__website = None
        self.__original = None
        self.__street = None
        self.__housenumber = None
        self.__conscriptionnumber = None
        self.__ref = None
        self.__phone = None
        self.__email = None
        self.__geom = None
        self.__latlong = None
        self.__nonstop = None
        self.__oh = pd.DataFrame(index=WeekDaysShort, columns=OpenClose)
        self.__mo_o = None
        self.__tu_o = None
        self.__we_o = None
        self.__th_o = None
        self.__fr_o = None
        self.__sa_o = None
        self.__su_o = None
        self.__mo_c = None
        self.__tu_c = None
        self.__we_c = None
        self.__th_c = None
        self.__fr_c = None
        self.__sa_c = None
        self.__su_c = None
        self.__summer_mo_o = None
        self.__summer_tu_o = None
        self.__summer_we_o = None
        self.__summer_th_o = None
        self.__summer_fr_o = None
        self.__summer_sa_o = None
        self.__summer_su_o = None
        self.__summer_mo_c = None
        self.__summer_tu_c = None
        self.__summer_we_c = None
        self.__summer_th_c = None
        self.__summer_fr_c = None
        self.__summer_sa_c = None
        self.__summer_su_c = None
        self.__lunch_break_start = None
        self.__lunch_break_stop = None
        self.__opening_hours = None

    @property
    def code(self):
        return (self.__code)

    @code.setter
    def code(self, data):
        self.__code = data

    @property
    def postcode(self):
        return (self.__postcode)

    @postcode.setter
    def postcode(self, data):
        self.__postcode = data

    @property
    def city(self):
        return (self.__city)

    @city.setter
    def city(self, data):
        self.__city = data

    @property
    def name(self):
        return (self.__name)

    @name.setter
    def name(self, data):
        self.__name = data

    @property
    def branch(self):
        return (self.__branch)

    @branch.setter
    def branch(self, data):
        self.__branch = data

    @property
    def website(self):
        return (self.__website)

    @website.setter
    def website(self, data):
        self.__website = data

    @property
    def original(self):
        return (self.__original)

    @original.setter
    def original(self, data):
        self.__original = data

    @property
    def street(self):
        return (self.__original)

    @street.setter
    def street(self, data):
        self.__street = data

    @property
    def housenumber(self):
        return (self.__original)

    @housenumber.setter
    def housenumber(self, data):
        self.__housenumber = data

    @property
    def conscriptionnumber(self):
        return (self.__original)

    @conscriptionnumber.setter
    def conscriptionnumber(self, data):
        self.__conscriptionnumber = data

    @property
    def ref(self):
        return (self.__original)

    @ref.setter
    def ref(self, data):
        self.__ref = data

    @property
    def phone(self):
        return (self.__original)

    @phone.setter
    def phone(self, data):
        self.__phone = data

    @property
    def email(self):
        return (self.__original)

    @email.setter
    def email(self, data):
        self.__email = data

    @property
    def geom(self):
        return (self.__original)

    @geom.setter
    def geom(self, data):
        self.__geom = data

    @property
    def latlong(self):
        return (self.__original)

    @latlong.setter
    def latlong(self, lat, long):
        self.__latlong = None

    @property
    def opening_hours_table(self):
        return (self.__oh)

    @opening_hours_table.setter
    def opening_hours_table(self, data):
       self.__oh = pd.DataFrame(data, index=WeekDaysShort, columns=OpenClose)

    @property
    def nonstop(self):
        return (self.__nonstop)

    @nonstop.setter
    def nonstop(self, data):
        self.__nonstop = data

    @property
    def mo_o(self):
        return (self.__mo_o)

    @website.setter
    def mo_o(self, data):
        self.__mo_o = data
        self.__oh.at[WeekDaysShort.mo, OpenClose.open] = data

    @property
    def tu_o(self):
        return (self.__tu_o)

    @website.setter
    def tu_o(self, data):
        self.__tu_o = data
        self.__oh.at[WeekDaysShort.tu, OpenClose.open] = data

    @property
    def we_o(self):
        return (self.__we_o)

    @website.setter
    def we_o(self, data):
        self.__we_o = data
        self.__oh.at[WeekDaysShort.we, OpenClose.open] = data

    @property
    def th_o(self):
        return (self.__th_o)

    @website.setter
    def th_o(self, data):
        self.__th_o = data
        self.__oh.at[WeekDaysShort.th, OpenClose.open] = data

    @property
    def fr_o(self):
        return (self.__fr_o)

    @website.setter
    def fr_o(self, data):
        self.__fr_o = data
        self.__oh.at[WeekDaysShort.fr, OpenClose.open] = data

    @property
    def sa_o(self):
        return (self.__sa_o)

    @website.setter
    def sa_o(self, data):
        self.__sa_o = data
        self.__oh.at[WeekDaysShort.sa, OpenClose.open] = data

    @property
    def su_o(self):
        return (self.__su_o)

    @website.setter
    def su_o(self, data):
        self.__su_o = data
        self.__oh.at[WeekDaysShort.su, OpenClose.open] = data

    @property
    def mo_c(self):
        return (self.__mo_c)

    @website.setter
    def mo_c(self, data):
        self.__mo_c = data
        self.__oh.at[WeekDaysShort.mo, OpenClose.close] = data

    @property
    def tu_c(self):
        return (self.__tu_c)

    @website.setter
    def tu_c(self, data):
        self.__tu_c = data
        self.__oh.at[WeekDaysShort.tu, OpenClose.close] = data

    @property
    def we_c(self):
        return (self.__we_c)

    @website.setter
    def we_c(self, data):
        self.__we_c = data
        self.__oh.at[WeekDaysShort.we, OpenClose.close] = data

    @property
    def th_c(self):
        return (self.__th_c)

    @website.setter
    def th_c(self, data):
        self.__th_c = data
        self.__oh.at[WeekDaysShort.th, OpenClose.close] = data

    @property
    def fr_c(self):
        return (self.__fr_c)

    @website.setter
    def fr_c(self, data):
        self.__fr_c = data
        self.__oh.at[WeekDaysShort.fr, OpenClose.close] = data

    @property
    def sa_c(self):
        return (self.__sa_c)

    @website.setter
    def sa_c(self, data):
        self.__sa_c = data
        self.__oh.at[WeekDaysShort.sa, OpenClose.close] = data

    @property
    def su_c(self):
        return (self.__su_c)

    @website.setter
    def su_c(self, data):
        self.__su_c = data
        self.__oh.at[WeekDaysShort.su, OpenClose.close] = data

    @property
    def summer_mo_o(self):
        return (self.__summer_mo_o)

    @website.setter
    def summer_mo_o(self, data):
        self.__summer_mo_o = data
        self.__oh.at[WeekDaysShort.mo, OpenClose.summer_open] = data

    @property
    def summer_tu_o(self):
        return (self.__summer_tu_o)

    @website.setter
    def summer_tu_o(self, data):
        self.__summer_tu_o = data
        self.__oh.at[WeekDaysShort.tu, OpenClose.summer_open] = data

    @property
    def summer_we_o(self):
        return (self.__summer_we_o)

    @website.setter
    def summer_we_o(self, data):
        self.__summer_we_o = data
        self.__oh.at[WeekDaysShort.we, OpenClose.summer_open] = data

    @property
    def summer_th_o(self):
        return (self.__summer_th_o)

    @website.setter
    def summer_th_o(self, data):
        self.__summer_th_o = data
        self.__oh.at[WeekDaysShort.th, OpenClose.summer_open] = data

    @property
    def summer_fr_o(self):
        return (self.__summer_fr_o)

    @website.setter
    def summer_fr_o(self, data):
        self.__summer_fr_o = data
        self.__oh.at[WeekDaysShort.fr, OpenClose.summer_open] = data

    @property
    def summer_sa_o(self):
        return (self.__summer_sa_o)

    @website.setter
    def summer_sa_o(self, data):
        self.__summer_sa_o = data
        self.__oh.at[WeekDaysShort.sa, OpenClose.summer_open] = data

    @property
    def summer_su_o(self):
        return (self.__summer_su_o)

    @website.setter
    def summer_su_o(self, data):
        self.__summer_su_o = data
        self.__oh.at[WeekDaysShort.su, OpenClose.summer_open] = data

    @property
    def summer_mo_c(self):
        return (self.__summer_mo_c)

    @website.setter
    def summer_mo_c(self, data):
        self.__summer_mo_c = data
        self.__oh.at[WeekDaysShort.mo, OpenClose.summer_close] = data

    @property
    def summer_tu_c(self):
        return (self.__summer_tu_c)

    @website.setter
    def summer_tu_c(self, data):
        self.__summer_tu_c = data
        self.__oh.at[WeekDaysShort.tu, OpenClose.summer_close] = data

    @property
    def summer_we_c(self):
        return (self.__summer_we_c)

    @website.setter
    def summer_we_c(self, data):
        self.__summer_we_c = data
        self.__oh.at[WeekDaysShort.we, OpenClose.summer_close] = data

    @property
    def summer_th_c(self):
        return (self.__summer_th_c)

    @website.setter
    def summer_th_c(self, data):
        self.__summer_th_c = data
        self.__oh.at[WeekDaysShort.th, OpenClose.summer_close] = data

    @property
    def summer_fr_c(self):
        return (self.__summer_fr_c)

    @website.setter
    def summer_fr_c(self, data):
        self.__summer_fr_c = data
        self.__oh.at[WeekDaysShort.fr, OpenClose.summer_close] = data

    @property
    def summer_sa_c(self):
        return (self.__summer_sa_c)

    @website.setter
    def summer_sa_c(self, data):
        self.__summer_sa_c = data
        self.__oh.at[WeekDaysShort.sa, OpenClose.summer_close] = data

    @property
    def summer_su_c(self):
        return (self.__summer_su_c)

    @website.setter
    def summer_su_c(self, data):
        self.__summer_su_c = data
        self.__oh.at[WeekDaysShort.su, OpenClose.summer_close] = data

    @property
    def lunch_break_start(self):
        return (self.__lunch_break_start)

    @lunch_break_start.setter
    def lunch_break_start(self, data):
        self.__lunch_break_start = data

    @property
    def lunch_break_stop(self):
        return (self.__lunch_break_stop)

    @lunch_break_stop.setter
    def lunch_break_stop(self, data):
        self.__lunch_break_stop = data

    @property
    def opening_hours(self):
        return (self.__opening_hours)

    @opening_hours.setter
    def opening_hours(self, data):
        self.__opening_hours = data

    def process_opening_hours(self):
        t = OpeningHours(self.__nonstop, self.__mo_o, self.__tu_o, self.__we_o, self.__th_o, self.__fr_o, self.__sa_o,
                         self.__su_o, self.__mo_c, self.__tu_c, self.__we_c, self.__th_c, self.__fr_c, self.__sa_c,
                         self.__su_c, self.__summer_mo_o, self.__summer_tu_o, self.__summer_we_o, self.__summer_th_o,
                         self.__summer_fr_o, self.__summer_sa_o, self.__summer_su_o, self.__summer_mo_c,
                         self.__summer_tu_c, self.__summer_we_c, self.__summer_th_c, self.__summer_fr_c,
                         self.__summer_sa_c, self.__summer_su_c,
                         self.__lunch_break_start, self.__lunch_break_stop)
        self.__opening_hours = t.process()

    def dump_opening_hours(self):
        print(self.__opening_hours)
