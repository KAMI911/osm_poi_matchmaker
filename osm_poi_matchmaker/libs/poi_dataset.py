# -*- coding: utf-8 -*-
__author__ = 'kami911'

try:
    import traceback
    import logging
    import sys
    import numpy as np
    import pandas as pd
    from osm_poi_matchmaker.utils.enums import WeekDaysShort, OpenClose, WeekDaysLongHU
    from osm_poi_matchmaker.libs.opening_hours import OpeningHours
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.libs.address import clean_string, clean_url, clean_branch
    from osm_poi_matchmaker.dao import poi_array_structure
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from osm_poi_matchmaker.libs.poi_qc import POIQC
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)

__program__ = 'poi_dataset'
__version__ = '0.0.5'

POI_COLS = poi_array_structure.POI_COLS


class POIDataset:

    def __init__(self):
        self.insert_data = []
        self.__db = POIBase(
            '{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                         config.get_database_writer_password(),
                                         config.get_database_writer_host(),
                                         config.get_database_writer_port(),
                                         config.get_database_poi_database()))
        self.clear_all()

    def clear_all(self):
        self.__code = None
        self.__postcode = None
        self.__city = None
        self.__name = None
        self.__branch = None
        self.__website = None
        self.__description = None
        self.__fuel_adblue = None
        self.__fuel_octane_100 = None
        self.__fuel_octane_98 = None
        self.__fuel_octane_95 = None
        self.__fuel_diesel_gtl = None
        self.__fuel_diesel = None
        self.__fuel_lpg = None
        self.__fuel_e85 = None
        self.__rent_lpg_bottles = None
        self.__compressed_air = None
        self.__restaurant = None
        self.__food = None
        self.__truck = None
        self.__original = None
        self.__street = None
        self.__housenumber = None
        self.__conscriptionnumber = None
        self.__ref = None
        self.__phone = None
        self.__email = None
        self.__geom = None
        self.__lat = None
        self.__lon = None
        self.__nonstop = None
        self.__oh = pd.DataFrame(index=WeekDaysShort, columns=OpenClose)
        self.__lunch_break = {'start': None, 'stop': None}
        self.__opening_hours = None
        self.__public_holiday_open = None
        self.__good = []
        self.__bad = []

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
        self.__branch = clean_branch(data)

    @property
    def website(self):
        return (self.__website)

    @website.setter
    def website(self, data):
        self.__website = clean_url(data)

    @property
    def description(self):
        return (self.__description)

    @description.setter
    def description(self, data):
        self.__description = data

    @property
    def fuel_adblue(self):
        return (self.__fuel_adblue)

    @fuel_adblue.setter
    def fuel_adblue(self, data):
        self.__fuel_adblue = data

    @property
    def fuel_octane_100(self):
        return (self.__fuel_octane_100)

    @fuel_octane_100.setter
    def fuel_octane_100(self, data):
        self.__fuel_octane_100 = data

    @property
    def fuel_octane_98(self):
        return (self.__fuel_octane_98)

    @fuel_octane_98.setter
    def fuel_octane_98(self, data):
        self.__fuel_octane_98 = data

    @property
    def fuel_octane_95(self):
        return (self.__fuel_octane_95)

    @fuel_octane_95.setter
    def fuel_octane_95(self, data):
        self.__fuel_octane_95 = data

    @property
    def fuel_diesel_gtl(self):
        return (self.__fuel_diesel_gtl)

    @fuel_diesel_gtl.setter
    def fuel_diesel_gtl(self, data):
        self.__fuel_diesel_gtl = data

    @property
    def fuel_diesel(self):
        return (self.__fuel_diesel)

    @fuel_diesel.setter
    def fuel_diesel(self, data):
        self.__fuel_diesel = data

    @property
    def fuel_lpg(self):
        return (self.__fuel_lpg)

    @fuel_lpg.setter
    def fuel_lpg(self, data):
        self.__fuel_lpg = data

    @property
    def fuel_e85(self):
        return (self.__fuel_e85)

    @fuel_e85.setter
    def fuel_e85(self, data):
        self.__fuel_e85 = data

    @property
    def rent_lpg_bottles(self):
        return (self.__rent_lpg_bottles)

    @rent_lpg_bottles.setter
    def rent_lpg_bottles(self, data):
        self.__rent_lpg_bottles = data

    @property
    def compressed_air(self):
        return (self.__compressed_air)

    @compressed_air.setter
    def compressed_air(self, data):
        self.__compressed_air = data

    @property
    def restaurant(self):
        return (self.__restaurant)

    @restaurant.setter
    def restaurant(self, data):
        self.__restaurant = data

    @property
    def food(self):
        return (self.__food)

    @food.setter
    def food(self, data):
        self.__food = data

    @property
    def truck(self):
        return (self.__truck)

    @truck.setter
    def truck(self, data):
        self.__truck = data

    @property
    def original(self):
        return (self.__original)

    @original.setter
    def original(self, data):
        self.__original = data

    @property
    def street(self):
        return (self.__street)

    @street.setter
    def street(self, data):
        # Try to find street name around
        if self.lat is not None and self.lon is not None:
            query = self.__db.query_name_and_metaphone_road_around(self.lon, self.lat, data, True)
            if query.empty:
                logging.warning('There is no street around named: {}'.format(data))
            else:
                logging.info('There is a street around named: {}'.format(data))
            self.__street = data

    @property
    def housenumber(self):
        return (self.__housenumber)

    @housenumber.setter
    def housenumber(self, data):
        self.__housenumber = data

    @property
    def conscriptionnumber(self):
        return (self.__conscriptionnumber)

    @conscriptionnumber.setter
    def conscriptionnumber(self, data):
        self.__conscriptionnumber = data

    @property
    def ref(self):
        return (self.__ref)

    @ref.setter
    def ref(self, data):
        self.__ref = data

    @property
    def phone(self):
        return (self.__phone)

    @phone.setter
    def phone(self, data):
        if data == 'NULL':
            self.__phone = None
        else:
            self.__phone = data

    @property
    def email(self):
        return (self.__email)

    @email.setter
    def email(self, data):
        if data == 'NULL':
            self.__email = None
        else:
            self.__email = data

    @property
    def geom(self):
        return (self.__geom)

    @geom.setter
    def geom(self, data):
        self.__geom = data

    @property
    def lat(self):
        return self.__lat

    @lat.setter
    def lat(self, lat):
        self.__lat = lat

    @property
    def lon(self):
        return self.__lon

    @lon.setter
    def lon(self, lon):
        self.__lon = lon

    def process_geom(self):
        self.geom = check_geom(self.__lat, self.__lon)

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
        return (self.__oh.at[WeekDaysShort.mo, OpenClose.open])

    @mo_o.setter
    def mo_o(self, data):
        self.__oh.at[WeekDaysShort.mo, OpenClose.open] = data

    @property
    def tu_o(self):
        return (self.__oh.at[WeekDaysShort.tu, OpenClose.open])

    @tu_o.setter
    def tu_o(self, data):
        self.__oh.at[WeekDaysShort.tu, OpenClose.open] = data

    @property
    def we_o(self):
        return (self.__oh.at[WeekDaysShort.we, OpenClose.open])

    @we_o.setter
    def we_o(self, data):
        self.__oh.at[WeekDaysShort.we, OpenClose.open] = data

    @property
    def th_o(self):
        return (self.__oh.at[WeekDaysShort.th, OpenClose.open])

    @th_o.setter
    def th_o(self, data):
        self.__oh.at[WeekDaysShort.th, OpenClose.open] = data

    @property
    def fr_o(self):
        return (self.__oh.at[WeekDaysShort.fr, OpenClose.open])

    @fr_o.setter
    def fr_o(self, data):
        self.__oh.at[WeekDaysShort.fr, OpenClose.open] = data

    @property
    def sa_o(self):
        return (self.__oh.at[WeekDaysShort.sa, OpenClose.open])

    @sa_o.setter
    def sa_o(self, data):
        self.__oh.at[WeekDaysShort.sa, OpenClose.open] = data

    @property
    def su_o(self):
        return (self.__oh.at[WeekDaysShort.su, OpenClose.open])

    @su_o.setter
    def su_o(self, data):
        self.__oh.at[WeekDaysShort.su, OpenClose.open] = data

    @property
    def mo_c(self):
        return (self.__oh.at[WeekDaysShort.mo, OpenClose.close])

    @mo_c.setter
    def mo_c(self, data):
        self.__oh.at[WeekDaysShort.mo, OpenClose.close] = data

    @property
    def tu_c(self):
        return (self.__oh.at[WeekDaysShort.tu, OpenClose.close])

    @tu_c.setter
    def tu_c(self, data):
        self.__oh.at[WeekDaysShort.tu, OpenClose.close] = data

    @property
    def we_c(self):
        return (self.__oh.at[WeekDaysShort.we, OpenClose.close])

    @we_c.setter
    def we_c(self, data):
        self.__oh.at[WeekDaysShort.we, OpenClose.close] = data

    @property
    def th_c(self):
        return (self.__oh.at[WeekDaysShort.th, OpenClose.close])

    @th_c.setter
    def th_c(self, data):
        self.__oh.at[WeekDaysShort.th, OpenClose.close] = data

    @property
    def fr_c(self):
        return (self.__oh.at[WeekDaysShort.fr, OpenClose.close])

    @fr_c.setter
    def fr_c(self, data):
        self.__oh.at[WeekDaysShort.fr, OpenClose.close] = data

    @property
    def sa_c(self):
        return (self.__oh.at[WeekDaysShort.sa, OpenClose.close])

    @sa_c.setter
    def sa_c(self, data):
        self.__oh.at[WeekDaysShort.sa, OpenClose.close] = data

    @property
    def su_c(self):
        return (self.__oh.at[WeekDaysShort.su, OpenClose.close])

    @su_c.setter
    def su_c(self, data):
        self.__oh.at[WeekDaysShort.su, OpenClose.close] = data

    @property
    def summer_mo_o(self):
        return (self.__oh.at[WeekDaysShort.mo, OpenClose.summer_open])

    @summer_mo_o.setter
    def summer_mo_o(self, data):
        self.__oh.at[WeekDaysShort.mo, OpenClose.summer_open] = data

    @property
    def summer_tu_o(self):
        return (self.__oh.at[WeekDaysShort.tu, OpenClose.summer_open])

    @summer_tu_o.setter
    def summer_tu_o(self, data):
        self.__oh.at[WeekDaysShort.tu, OpenClose.summer_open] = data

    @property
    def summer_we_o(self):
        return (self.__oh.at[WeekDaysShort.we, OpenClose.summer_open])

    @summer_we_o.setter
    def summer_we_o(self, data):
        self.__oh.at[WeekDaysShort.we, OpenClose.summer_open] = data

    @property
    def summer_th_o(self):
        return (self.__oh.at[WeekDaysShort.th, OpenClose.summer_open])

    @summer_th_o.setter
    def summer_th_o(self, data):
        self.__oh.at[WeekDaysShort.th, OpenClose.summer_open] = data

    @property
    def summer_fr_o(self):
        return (self.__oh.at[WeekDaysShort.fr, OpenClose.summer_open])

    @summer_fr_o.setter
    def summer_fr_o(self, data):
        self.__oh.at[WeekDaysShort.fr, OpenClose.summer_open] = data

    @property
    def summer_sa_o(self):
        return (self.__oh.at[WeekDaysShort.sa, OpenClose.summer_open])

    @summer_sa_o.setter
    def summer_sa_o(self, data):
        self.__oh.at[WeekDaysShort.sa, OpenClose.summer_open] = data

    @property
    def summer_su_o(self):
        return (self.__oh.at[WeekDaysShort.su, OpenClose.summer_open])

    @summer_su_o.setter
    def summer_su_o(self, data):
        self.__oh.at[WeekDaysShort.su, OpenClose.summer_open] = data

    @property
    def summer_mo_c(self):
        return (self.__oh.at[WeekDaysShort.mo, OpenClose.summer_close])

    @summer_mo_c.setter
    def summer_mo_c(self, data):
        self.__oh.at[WeekDaysShort.mo, OpenClose.summer_close] = data

    @property
    def summer_tu_c(self):
        return (self.__oh.at[WeekDaysShort.tu, OpenClose.summer_close])

    @summer_tu_c.setter
    def summer_tu_c(self, data):
        self.__oh.at[WeekDaysShort.tu, OpenClose.summer_close] = data

    @property
    def summer_we_c(self):
        return (self.__oh.at[WeekDaysShort.we, OpenClose.summer_close])

    @summer_we_c.setter
    def summer_we_c(self, data):
        self.__oh.at[WeekDaysShort.we, OpenClose.summer_close] = data

    @property
    def summer_th_c(self):
        return (self.__oh.at[WeekDaysShort.th, OpenClose.summer_close])

    @summer_th_c.setter
    def summer_th_c(self, data):
        self.__oh.at[WeekDaysShort.th, OpenClose.summer_close] = data

    @property
    def summer_fr_c(self):
        return (self.__oh.at[WeekDaysShort.fr, OpenClose.summer_close])

    @summer_fr_c.setter
    def summer_fr_c(self, data):
        self.__oh.at[WeekDaysShort.fr, OpenClose.summer_close] = data

    @property
    def summer_sa_c(self):
        return (self.__oh.at[WeekDaysShort.sa, OpenClose.summer_close])

    @summer_sa_c.setter
    def summer_sa_c(self, data):
        self.__oh.at[WeekDaysShort.sa, OpenClose.summer_close] = data

    @property
    def summer_su_c(self):
        return (self.__oh.at[WeekDaysShort.su, OpenClose.summer_close])

    @summer_su_c.setter
    def summer_su_c(self, data):
        self.__oh.at[WeekDaysShort.su, OpenClose.summer_close] = data

    @property
    def lunch_break(self):
        return (self.__lunch_break['start'], self.__lunch_break['stop'])

    @lunch_break.setter
    def lunch_break(self, lunch_break_start, lunch_break_stop):
        self.__lunch_break = {'start': lunch_break_start, 'stop': lunch_break_stop}

    @property
    def lunch_break_start(self):
        return (self.__lunch_break['start'])

    @lunch_break_start.setter
    def lunch_break_start(self, data):
        self.__lunch_break['start'] = data

    @property
    def lunch_break_stop(self):
        return (self.__lunch_break['stop'])

    @lunch_break_stop.setter
    def lunch_break_stop(self, data):
        self.__lunch_break['stop'] = data

    def day_open(self, day, data):
        self.__oh.at[WeekDaysShort(day), OpenClose.open] = data

    def day_close(self, day, data):
        self.__oh.at[WeekDaysShort(day), OpenClose.close] = data

    def day_summer_open(self, day, data):
        self.__oh.at[WeekDaysShort(day), OpenClose.summer_open] = data

    def day_summer_close(self, day, data):
        self.__oh.at[WeekDaysShort(day), OpenClose.summer_close] = data

    def day_open_close(self, day, opening, closing):
        self.__oh.at[WeekDaysShort(day), OpenClose.open] = opening
        self.__oh.at[WeekDaysShort(day), OpenClose.close] = closing

    def day_summer_open_close(self, day, opening, closing):
        self.__oh.at[WeekDaysShort(day), OpenClose.summer_open] = opening
        self.__oh.at[WeekDaysShort(day), OpenClose.summer_close] = closing

    @property
    def opening_hours(self):
        return (self.__opening_hours)

    @opening_hours.setter
    def opening_hours(self, data):
        self.__opening_hours = data

    @property
    def public_holiday_open(self):
        return (self.__public_holiday_open)

    @public_holiday_open.setter
    def public_holiday_open(self, data):
        self.__public_holiday_open = data

    def process_opening_hours(self):
        self.__oh = self.__oh.where((pd.notnull(self.__oh)), None)
        t = OpeningHours(self.__nonstop, self.__oh.at[WeekDaysShort.mo, OpenClose.open],
                         self.__oh.at[WeekDaysShort.tu, OpenClose.open],
                         self.__oh.at[WeekDaysShort.we, OpenClose.open],
                         self.__oh.at[WeekDaysShort.th, OpenClose.open],
                         self.__oh.at[WeekDaysShort.fr, OpenClose.open],
                         self.__oh.at[WeekDaysShort.sa, OpenClose.open],
                         self.__oh.at[WeekDaysShort.su, OpenClose.open],
                         self.__oh.at[WeekDaysShort.mo, OpenClose.close],
                         self.__oh.at[WeekDaysShort.tu, OpenClose.close],
                         self.__oh.at[WeekDaysShort.we, OpenClose.close],
                         self.__oh.at[WeekDaysShort.th, OpenClose.close],
                         self.__oh.at[WeekDaysShort.fr, OpenClose.close],
                         self.__oh.at[WeekDaysShort.sa, OpenClose.close],
                         self.__oh.at[WeekDaysShort.su, OpenClose.close],
                         self.__oh.at[WeekDaysShort.mo, OpenClose.summer_open],
                         self.__oh.at[WeekDaysShort.tu, OpenClose.summer_open],
                         self.__oh.at[WeekDaysShort.we, OpenClose.summer_open],
                         self.__oh.at[WeekDaysShort.th, OpenClose.summer_open],
                         self.__oh.at[WeekDaysShort.fr, OpenClose.summer_open],
                         self.__oh.at[WeekDaysShort.sa, OpenClose.summer_open],
                         self.__oh.at[WeekDaysShort.su, OpenClose.summer_open],
                         self.__oh.at[WeekDaysShort.mo, OpenClose.summer_close],
                         self.__oh.at[WeekDaysShort.tu, OpenClose.summer_close],
                         self.__oh.at[WeekDaysShort.we, OpenClose.summer_close],
                         self.__oh.at[WeekDaysShort.th, OpenClose.summer_close],
                         self.__oh.at[WeekDaysShort.fr, OpenClose.summer_close],
                         self.__oh.at[WeekDaysShort.sa, OpenClose.summer_close],
                         self.__oh.at[WeekDaysShort.su, OpenClose.summer_close],
                         self.__lunch_break['start'], self.__lunch_break['stop'],
                         self.__public_holiday_open)
        self.__opening_hours = t.process()

    def dump_opening_hours(self):
        print(self.__opening_hours)

    def add(self):
        try:
            self.process_opening_hours()
            self.process_geom()
            pqc = POIQC(self.__db, self.__lon, self.__lat, self.__opening_hours, self.__street)
            self.__good, self.__bad = pqc.process()
            self.insert_data.append(
                [self.__code, self.__postcode, self.__city, self.__name, clean_string(self.__branch), self.__website,
                 self.__description, self.__fuel_adblue, self.__fuel_octane_100, self.__fuel_octane_98,
                 self.__fuel_octane_95, self.__fuel_diesel_gtl, self.__fuel_diesel, self.__fuel_lpg,
                 self.__fuel_e85, self.__rent_lpg_bottles, self.__compressed_air, self.__restaurant, self.__food,
                 self.__truck,
                 self.__original, self.__street, self.__housenumber, self.__conscriptionnumber,
                 self.__ref, self.__phone, self.__email, self.__geom, self.__nonstop,
                 self.__oh.at[WeekDaysShort.mo, OpenClose.open],
                 self.__oh.at[WeekDaysShort.tu, OpenClose.open],
                 self.__oh.at[WeekDaysShort.we, OpenClose.open],
                 self.__oh.at[WeekDaysShort.th, OpenClose.open],
                 self.__oh.at[WeekDaysShort.fr, OpenClose.open],
                 self.__oh.at[WeekDaysShort.sa, OpenClose.open],
                 self.__oh.at[WeekDaysShort.su, OpenClose.open],
                 self.__oh.at[WeekDaysShort.mo, OpenClose.close],
                 self.__oh.at[WeekDaysShort.tu, OpenClose.close],
                 self.__oh.at[WeekDaysShort.we, OpenClose.close],
                 self.__oh.at[WeekDaysShort.th, OpenClose.close],
                 self.__oh.at[WeekDaysShort.fr, OpenClose.close],
                 self.__oh.at[WeekDaysShort.sa, OpenClose.close],
                 self.__oh.at[WeekDaysShort.su, OpenClose.close],
                 self.__oh.at[WeekDaysShort.mo, OpenClose.summer_open],
                 self.__oh.at[WeekDaysShort.tu, OpenClose.summer_open],
                 self.__oh.at[WeekDaysShort.we, OpenClose.summer_open],
                 self.__oh.at[WeekDaysShort.th, OpenClose.summer_open],
                 self.__oh.at[WeekDaysShort.fr, OpenClose.summer_open],
                 self.__oh.at[WeekDaysShort.sa, OpenClose.summer_open],
                 self.__oh.at[WeekDaysShort.su, OpenClose.summer_open],
                 self.__oh.at[WeekDaysShort.mo, OpenClose.summer_close],
                 self.__oh.at[WeekDaysShort.tu, OpenClose.summer_close],
                 self.__oh.at[WeekDaysShort.we, OpenClose.summer_close],
                 self.__oh.at[WeekDaysShort.th, OpenClose.summer_close],
                 self.__oh.at[WeekDaysShort.fr, OpenClose.summer_close],
                 self.__oh.at[WeekDaysShort.sa, OpenClose.summer_close],
                 self.__oh.at[WeekDaysShort.su, OpenClose.summer_close], self.__lunch_break['start'],
                 self.__lunch_break['stop'],
                 self.__public_holiday_open, self.__opening_hours, self.__good, self.__bad])
            self.clear_all()
        except Exception as err:
            logging.error(err)
            logging.error(traceback.print_exc())

    def process(self):
        df = pd.DataFrame(self.insert_data)
        df.columns = POI_COLS
        return df.where((pd.notnull(df)), None)

    def lenght(self):
        return len(self.insert_data)
