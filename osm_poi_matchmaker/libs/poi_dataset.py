# -*- coding: utf-8 -*-
__author__ = 'kami911'

try:
    import logging
    import sys
    import numpy as np
    import pandas as pd
    from osm_poi_matchmaker.utils.enums import WeekDaysShort, OpenClose
    from osm_poi_matchmaker.libs.opening_hours import OpeningHours
    from osm_poi_matchmaker.libs.geo import check_geom
    from osm_poi_matchmaker.libs.address import clean_string, clean_url, clean_branch, clean_phone_to_str, clean_email
    from osm_poi_matchmaker.dao import poi_array_structure
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.dao.poi_base import POIBase
    from osm_poi_matchmaker.libs.poi_qc import POIQC
except ImportError as err:
    logging.error('Error %s import module: %s', module=__name__, error=err)
    logging.exception('Exception occurred')

    sys.exit(128)

__program__ = 'poi_dataset'
__version__ = '0.0.5'

POI_COLS = poi_array_structure.POI_COLS
POI_COLS_RAW = poi_array_structure.POI_COLS_RAW
INTEGER_GEO = 100000

class POIDatasetRaw:
    """Contains all handled OSM tags
    """
    def __init__(self):
        """
        """
        self.insert_data = []
        self.__db = POIBase(
            '{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                         config.get_database_writer_password(),
                                         config.get_database_writer_host(),
                                         config.get_database_writer_port(),
                                         config.get_database_poi_database()))
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
        self.__authentication_app = None
        self.__authentication_membership_card = None
        self.__capacity = None
        self.__fee = None
        self.__parking_fee = None
        self.__motorcar = None
        self.__socket_chademo = None
        self.__socket_chademo_output = None
        self.__socket_type2_combo = None
        self.__socket_type2_combo_output = None
        self.__socket_type2_cable = None
        self.__socket_type2_cable_output = None
        self.__socket_type2 = None
        self.__socket_type2_output = None
        self.__manufacturer = None
        self.__model = None
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
        self.__lunch_break_start = None
        self.__lunch_break_stop = None
        self.__opening_hours = None
        self.__public_holiday_open = None

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
        self.__authentication_app = None
        self.__authentication_membership_card = None
        self.__capacity = None
        self.__fee = None
        self.__parking_fee = None
        self.__motorcar = None
        self.__socket_chademo = None
        self.__socket_chademo_output = None
        self.__socket_type2_combo = None
        self.__socket_type2_combo_output = None
        self.__socket_type2_cable = None
        self.__socket_type2_cable_output = None
        self.__socket_type2 = None
        self.__socket_type2_output = None
        self.__manufacturer = None
        self.__model = None
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
        self.__lunch_break_start = None
        self.__lunch_break_stop = None
        self.__opening_hours = None
        self.__public_holiday_open = None

    @property
    def code(self) -> str:
        return self.__code

    @code.setter
    def code(self, data: str):
        self.__code = clean_string(data)

    @property
    def postcode(self) -> int:
        return self.__postcode

    @postcode.setter
    def postcode(self, data: str):
        self.__postcode = clean_string(data)

    @property
    def city(self) -> str:
        return self.__city

    @city.setter
    def city(self, data: str):
        self.__city = clean_string(data)

    @property
    def name(self) -> str:
        return self.__name

    @name.setter
    def name(self, data: str):
        self.__name = clean_string(data)

    @property
    def branch(self) -> str:
        return self.__branch

    @branch.setter
    def branch(self, data: str):
        self.__branch = clean_branch(data)

    @property
    def website(self) -> str:
        return self.__website

    @website.setter
    def website(self, data: str):
        self.__website = clean_url(data)

    @property
    def description(self) -> str:
        return self.__description

    @description.setter
    def description(self, data: str):
        self.__description = clean_string(data)

    @property
    def fuel_adblue(self) -> bool:
        return self.__fuel_adblue

    @fuel_adblue.setter
    def fuel_adblue(self, data: bool):
        self.__fuel_adblue = data

    @property
    def fuel_octane_100(self) -> bool:
        return self.__fuel_octane_100

    @fuel_octane_100.setter
    def fuel_octane_100(self, data: bool):
        self.__fuel_octane_100 = data

    @property
    def fuel_octane_98(self) -> bool:
        return self.__fuel_octane_98

    @fuel_octane_98.setter
    def fuel_octane_98(self, data: bool):
        self.__fuel_octane_98 = data

    @property
    def fuel_octane_95(self) -> bool:
        return self.__fuel_octane_95

    @fuel_octane_95.setter
    def fuel_octane_95(self, data: bool):
        self.__fuel_octane_95 = data

    @property
    def fuel_diesel_gtl(self) -> bool:
        return self.__fuel_diesel_gtl

    @fuel_diesel_gtl.setter
    def fuel_diesel_gtl(self, data: bool):
        self.__fuel_diesel_gtl = data

    @property
    def fuel_diesel(self) -> bool:
        return self.__fuel_diesel

    @fuel_diesel.setter
    def fuel_diesel(self, data: bool):
        self.__fuel_diesel = data

    @property
    def fuel_lpg(self) -> bool:
        return self.__fuel_lpg

    @fuel_lpg.setter
    def fuel_lpg(self, data: bool):
        self.__fuel_lpg = data

    @property
    def fuel_e85(self) -> bool:
        return self.__fuel_e85

    @fuel_e85.setter
    def fuel_e85(self, data: bool):
        self.__fuel_e85 = data

    @property
    def rent_lpg_bottles(self) -> bool:
        return self.__rent_lpg_bottles

    @rent_lpg_bottles.setter
    def rent_lpg_bottles(self, data: bool):
        self.__rent_lpg_bottles = data

    @property
    def compressed_air(self) -> bool:
        return self.__compressed_air

    @compressed_air.setter
    def compressed_air(self, data: bool):
        self.__compressed_air = data

    @property
    def restaurant(self) -> bool:
        return self.__restaurant

    @restaurant.setter
    def restaurant(self, data: bool):
        self.__restaurant = data

    @property
    def food(self) -> bool:
        return self.__food

    @food.setter
    def food(self, data: bool):
        self.__food = data

    @property
    def truck(self) -> bool:
        return self.__truck

    @truck.setter
    def truck(self, data: bool):
        self.__truck = data

    @property
    def authentication_app(self) -> bool:
        return self.__authentication_app

    @authentication_app.setter
    def authentication_app(self, data: bool):
        self.__authentication_app = data

    @property
    def authentication_membership_card(self) -> bool:
        return self.__authentication_membership_card

    @authentication_membership_card.setter
    def authentication_membership_card(self, data: bool):
        self.__authentication_membership_card = data

    @property
    def capacity(self) -> int:
        return self.__capacity

    @capacity.setter
    def capacity(self, data: int):
        if clean_string(data) is not None:
            self.__capacity = int(float(clean_string(data)))

    @property
    def fee(self) -> bool:
        return self.__fee

    @fee.setter
    def fee(self, data: bool):
        self.__fee = data

    @property
    def parking_fee(self):
        return self.__parking_fee

    @parking_fee.setter
    def parking_fee(self, data: bool):
        self.__parking_fee = data

    @property
    def motorcar(self) -> bool:
        return self.__motorcar

    @motorcar.setter
    def motorcar(self, data: bool):
        self.__motorcar = data

    @property
    def socket_chademo(self) -> int:
        return int(self.__socket_chademo)

    @socket_chademo.setter
    def socket_chademo(self, data: int):
        if clean_string(data) is not None:
            self.__socket_chademo = int(float(clean_string(data)))

    @property
    def socket_chademo_output(self) -> str:
        return self.__socket_chademo_output

    @socket_chademo_output.setter
    def socket_chademo_output(self, data: str):
        self.__socket_chademo_output = clean_string(data)

    @property
    def socket_type2_combo(self) -> int:
        return int(self.__socket_type2_combo)

    @socket_type2_combo.setter
    def socket_type2_combo(self, data: int):
        if clean_string(data) is not None:
            self.__socket_type2_combo = int(float(clean_string(data)))

    @property
    def socket_type2_combo_output(self) -> str:
        return self.__socket_type2_combo_output

    @socket_type2_combo_output.setter
    def socket_type2_combo_output(self, data: str):
        self.__socket_type2_combo_output = clean_string(data)

    @property
    def socket_type2_cable(self) -> int:
        return int(self.__socket_type2_cable)

    @socket_type2_cable.setter
    def socket_type2_cable(self, data: int):
        if clean_string(data) is not None:
            self.__socket_type2_cable = int(float(clean_string(data)))

    @property
    def socket_type2_cable_output(self) -> str:
        return self.__socket_type2_cable_output

    @socket_type2_cable_output.setter
    def socket_type2_cable_output(self, data: str):
        self.__socket_type2_cable_output = clean_string(data)

    @property
    def socket_type2(self) -> int:
        return int(self.__socket_type2)

    @socket_type2.setter
    def socket_type2(self, data: int):
        if clean_string(data) is not None:
            self.__socket_type2 = int(float(clean_string(data)))

    @property
    def socket_type2_output(self) -> str:
        return self.__socket_type2_output

    @socket_type2_output.setter
    def socket_type2_output(self, data: str):
        self.__socket_type2_output = clean_string(data)

    @property
    def manufacturer(self) -> str:
        return self.__manufacturer

    @manufacturer.setter
    def manufacturer(self, data: str):
        self.__manufacturer = clean_string(data)

    @property
    def model(self) -> str:
        return self.__model

    @model.setter
    def model(self, data: str):
        self.__model = clean_string(data)

    @property
    def original(self) -> str:
        return self.__original

    @original.setter
    def original(self, data: str):
        self.__original = clean_string(data)

    @property
    def street(self) -> str:
        return self.__street

    '''@street.setter
    def street(self, data: str):
        self.__street = clean_string(data)
    '''
    # Temporary street locator for final check TODO: remove when two phase save is active
    @street.setter
    def street(self, data: str):
        # Try to find street name around
        try:
            logging.debug('Checking street name ...')
            if self.__lat is not None and self.__lon is not None:
                query = self.__db.query_name_road_around(self.__lon, self.__lat, data, True, 'name')
                if query is None or query.empty:
                    query = self.__db.query_name_road_around(self.__lon, self.__lat, data, True, 'metaphone')
                    if query is None or query.empty:
                        logging.warning('There is no street around named or metaphone named: %s', data)
                        self.__street = data
                    else:
                        new_data = query.at[0, 'name']
                        logging.info('There is a metaphone street around named: %s, original was: %s.', new_data, data)
                        self.__street = new_data
                else:
                    logging.info('There is a street around named: %s.', data)
                    self.__street = data
            else:
                logging.debug('There are not coordinates. Is this a bug or missing data?')
                self.__street = data
        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')

    @property
    def housenumber(self) -> str:
        return self.__housenumber

    @housenumber.setter
    def housenumber(self, data: str):
        self.__housenumber = clean_string(data)

    @property
    def conscriptionnumber(self) -> str:
        return self.__conscriptionnumber

    @conscriptionnumber.setter
    def conscriptionnumber(self, data: str):
        self.__conscriptionnumber = clean_string(data)

    @property
    def ref(self) -> str:
        return self.__ref

    @ref.setter
    def ref(self, data: str):
        self.__ref = clean_string(data)

    @property
    def phone(self) -> str:
        return self.__phone

    @phone.setter
    def phone(self, data: str):
        self.__phone = clean_phone_to_str(data)

    @property
    def email(self) -> str:
        return self.__email

    @email.setter
    def email(self, data: str):
        self.__email = clean_email(data)

    @property
    def geom(self) -> str:
        return self.__geom

    @geom.setter
    def geom(self, data: str):
        self.__geom = data

    @property
    def lat(self) -> float:
        return self.__lat

    @lat.setter
    def lat(self, lat: float):
        self.__lat = lat

    @property
    def lon(self) -> float:
        return self.__lon

    @lon.setter
    def lon(self, lon: float):
        self.__lon = lon

    def process_geom(self):
        self.geom = check_geom(self.__lat, self.__lon)

    @property
    def opening_hours_table(self):
        return self.__oh

    @opening_hours_table.setter
    def opening_hours_table(self, data):
        self.__oh = pd.DataFrame(data, index=WeekDaysShort, columns=OpenClose)

    @property
    def nonstop(self) -> bool:
        return self.__nonstop

    @nonstop.setter
    def nonstop(self, data: bool):
        self.__nonstop = data

    @property
    def mo_o(self) -> str:
        return self.__oh.at[WeekDaysShort.mo, OpenClose.open]

    @mo_o.setter
    def mo_o(self, data: str):
        self.__oh.at[WeekDaysShort.mo, OpenClose.open] = data

    @property
    def tu_o(self) -> str:
        return self.__oh.at[WeekDaysShort.tu, OpenClose.open]

    @tu_o.setter
    def tu_o(self, data: str):
        self.__oh.at[WeekDaysShort.tu, OpenClose.open] = data

    @property
    def we_o(self) -> str:
        return self.__oh.at[WeekDaysShort.we, OpenClose.open]

    @we_o.setter
    def we_o(self, data: str):
        self.__oh.at[WeekDaysShort.we, OpenClose.open] = data

    @property
    def th_o(self) -> str:
        return self.__oh.at[WeekDaysShort.th, OpenClose.open]

    @th_o.setter
    def th_o(self, data: str):
        self.__oh.at[WeekDaysShort.th, OpenClose.open] = data

    @property
    def fr_o(self) -> str:
        return self.__oh.at[WeekDaysShort.fr, OpenClose.open]

    @fr_o.setter
    def fr_o(self, data: str):
        self.__oh.at[WeekDaysShort.fr, OpenClose.open] = data

    @property
    def sa_o(self) -> str:
        return self.__oh.at[WeekDaysShort.sa, OpenClose.open]

    @sa_o.setter
    def sa_o(self, data: str):
        self.__oh.at[WeekDaysShort.sa, OpenClose.open] = data

    @property
    def su_o(self) -> str:
        return self.__oh.at[WeekDaysShort.su, OpenClose.open]

    @su_o.setter
    def su_o(self, data: str):
        self.__oh.at[WeekDaysShort.su, OpenClose.open] = data

    @property
    def mo_c(self) -> str:
        return self.__oh.at[WeekDaysShort.mo, OpenClose.close]

    @mo_c.setter
    def mo_c(self, data: str):
        self.__oh.at[WeekDaysShort.mo, OpenClose.close] = data

    @property
    def tu_c(self) -> str:
        return self.__oh.at[WeekDaysShort.tu, OpenClose.close]

    @tu_c.setter
    def tu_c(self, data: str):
        self.__oh.at[WeekDaysShort.tu, OpenClose.close] = data

    @property
    def we_c(self) -> str:
        return self.__oh.at[WeekDaysShort.we, OpenClose.close]

    @we_c.setter
    def we_c(self, data: str):
        self.__oh.at[WeekDaysShort.we, OpenClose.close] = data

    @property
    def th_c(self) -> str:
        return self.__oh.at[WeekDaysShort.th, OpenClose.close]

    @th_c.setter
    def th_c(self, data: str):
        self.__oh.at[WeekDaysShort.th, OpenClose.close] = data

    @property
    def fr_c(self) -> str:
        return self.__oh.at[WeekDaysShort.fr, OpenClose.close]

    @fr_c.setter
    def fr_c(self, data: str):
        self.__oh.at[WeekDaysShort.fr, OpenClose.close] = data

    @property
    def sa_c(self) -> str:
        return self.__oh.at[WeekDaysShort.sa, OpenClose.close]

    @sa_c.setter
    def sa_c(self, data: str):
        self.__oh.at[WeekDaysShort.sa, OpenClose.close] = data

    @property
    def su_c(self) -> str:
        return self.__oh.at[WeekDaysShort.su, OpenClose.close]

    @su_c.setter
    def su_c(self, data: str):
        self.__oh.at[WeekDaysShort.su, OpenClose.close] = data

    @property
    def summer_mo_o(self) -> str:
        return self.__oh.at[WeekDaysShort.mo, OpenClose.summer_open]

    @summer_mo_o.setter
    def summer_mo_o(self, data: str):
        self.__oh.at[WeekDaysShort.mo, OpenClose.summer_open] = data

    @property
    def summer_tu_o(self) -> str:
        return self.__oh.at[WeekDaysShort.tu, OpenClose.summer_open]

    @summer_tu_o.setter
    def summer_tu_o(self, data: str):
        self.__oh.at[WeekDaysShort.tu, OpenClose.summer_open] = data

    @property
    def summer_we_o(self) -> str:
        return self.__oh.at[WeekDaysShort.we, OpenClose.summer_open]

    @summer_we_o.setter
    def summer_we_o(self, data: str):
        self.__oh.at[WeekDaysShort.we, OpenClose.summer_open] = data

    @property
    def summer_th_o(self) -> str:
        return self.__oh.at[WeekDaysShort.th, OpenClose.summer_open]

    @summer_th_o.setter
    def summer_th_o(self, data: str):
        self.__oh.at[WeekDaysShort.th, OpenClose.summer_open] = data

    @property
    def summer_fr_o(self) -> str:
        return self.__oh.at[WeekDaysShort.fr, OpenClose.summer_open]

    @summer_fr_o.setter
    def summer_fr_o(self, data: str):
        self.__oh.at[WeekDaysShort.fr, OpenClose.summer_open] = data

    @property
    def summer_sa_o(self) -> str:
        return self.__oh.at[WeekDaysShort.sa, OpenClose.summer_open]

    @summer_sa_o.setter
    def summer_sa_o(self, data: str):
        self.__oh.at[WeekDaysShort.sa, OpenClose.summer_open] = data

    @property
    def summer_su_o(self) -> str:
        return self.__oh.at[WeekDaysShort.su, OpenClose.summer_open]

    @summer_su_o.setter
    def summer_su_o(self, data: str):
        self.__oh.at[WeekDaysShort.su, OpenClose.summer_open] = data

    @property
    def summer_mo_c(self) -> str:
        return self.__oh.at[WeekDaysShort.mo, OpenClose.summer_close]

    @summer_mo_c.setter
    def summer_mo_c(self, data: str):
        self.__oh.at[WeekDaysShort.mo, OpenClose.summer_close] = data

    @property
    def summer_tu_c(self) -> str:
        return self.__oh.at[WeekDaysShort.tu, OpenClose.summer_close]

    @summer_tu_c.setter
    def summer_tu_c(self, data: str):
        self.__oh.at[WeekDaysShort.tu, OpenClose.summer_close] = data

    @property
    def summer_we_c(self) -> str:
        return self.__oh.at[WeekDaysShort.we, OpenClose.summer_close]

    @summer_we_c.setter
    def summer_we_c(self, data: str):
        self.__oh.at[WeekDaysShort.we, OpenClose.summer_close] = data

    @property
    def summer_th_c(self) -> str:
        return self.__oh.at[WeekDaysShort.th, OpenClose.summer_close]

    @summer_th_c.setter
    def summer_th_c(self, data: str):
        self.__oh.at[WeekDaysShort.th, OpenClose.summer_close] = data

    @property
    def summer_fr_c(self) -> str:
        return self.__oh.at[WeekDaysShort.fr, OpenClose.summer_close]

    @summer_fr_c.setter
    def summer_fr_c(self, data: str):
        self.__oh.at[WeekDaysShort.fr, OpenClose.summer_close] = data

    @property
    def summer_sa_c(self) -> str:
        return self.__oh.at[WeekDaysShort.sa, OpenClose.summer_close]

    @summer_sa_c.setter
    def summer_sa_c(self, data: str):
        self.__oh.at[WeekDaysShort.sa, OpenClose.summer_close] = data

    @property
    def summer_su_c(self) -> str:
        return self.__oh.at[WeekDaysShort.su, OpenClose.summer_close]

    @summer_su_c.setter
    def summer_su_c(self, data: str):
        self.__oh.at[WeekDaysShort.su, OpenClose.summer_close] = data

    @property
    def lunch_break_start(self) -> str:
        return self.__lunch_break_start

    @lunch_break_start.setter
    def lunch_break_start(self, data: str):
        self.__lunch_break_start = data

    @property
    def lunch_break_stop(self) -> str:
        return self.__lunch_break_stop

    @lunch_break_stop.setter
    def lunch_break_stop(self, data: str):
        self.__lunch_break_stop = data

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
    def opening_hours(self) -> str:
        return self.__opening_hours

    @opening_hours.setter
    def opening_hours(self, data: str):
        self.__opening_hours = data

    @property
    def public_holiday_open(self) -> bool:
        return self.__public_holiday_open

    @public_holiday_open.setter
    def public_holiday_open(self, data: bool):
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
                         self.__lunch_break_start, self.__lunch_break_stop,
                         self.__public_holiday_open)
        self.__opening_hours = t.process()

    def dump_opening_hours(self):
        print(self.__opening_hours)

    def add(self):
        try:
            self.process_opening_hours()
            self.process_geom()
            self.insert_data.append(
                [self.__code, self.__postcode, self.__city, self.__name, clean_string(self.__branch), self.__website,
                 self.__description, self.__fuel_adblue, self.__fuel_octane_100, self.__fuel_octane_98,
                 self.__fuel_octane_95, self.__fuel_diesel_gtl, self.__fuel_diesel, self.__fuel_lpg,
                 self.__fuel_e85, self.__rent_lpg_bottles, self.__compressed_air, self.__restaurant, self.__food,
                 self.__truck,
                 self.__authentication_app, self.__authentication_membership_card, self.__capacity, self.__fee,
                 self.__parking_fee, self.__motorcar, self.__socket_chademo, self.__socket_chademo_output,
                 self.__socket_type2_combo, self.__socket_type2_combo_output,
                 self.__socket_type2_cable, self.__socket_type2_cable_output,
                 self.__socket_type2, self.__socket_type2_output, self.__manufacturer, self.__model,
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
                 self.__oh.at[WeekDaysShort.su, OpenClose.summer_close], self.__lunch_break_start,
                 self.__lunch_break_stop,
                 self.__public_holiday_open, self.__opening_hours, self.__lat, self.__lon ])
            self.clear_all()
        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')

    def process(self):
        df = pd.DataFrame(self.insert_data)
        df.columns = POI_COLS_RAW
        return df.replace(np.nan:None)

    def lenght(self):
        return len(self.insert_data)

class POIDataset(POIDatasetRaw):
    """Contains all handled OSM tags
    """
    def __init__(self):
        """
        """
        POIDatasetRaw.__init__(self)
        self.__db = POIBase(
            '{}://{}:{}@{}:{}/{}'.format(config.get_database_type(), config.get_database_writer_username(),
                                         config.get_database_writer_password(),
                                         config.get_database_writer_host(),
                                         config.get_database_writer_port(),
                                         config.get_database_poi_database()))
        self.__good = []
        self.__bad = []

    def clear_all(self):
        POIDatasetRaw.clear_all(self)
        self.__good = []
        self.__bad = []


    @property
    def street(self) -> str:
        return self.__street

    @street.setter
    def street(self, data: str):
        # Try to find street name around
        try:
            logging.debug('Checking street name ...')
            if self.__lat is not None and self.__lon is not None:
                query = self.__db.query_name_road_around(self.__lon, self.__lat, data, True, 'name')
                if query is None or query.empty:
                    query = self.__db.query_name_road_around(self.__lon, self.__lat, data, True, 'metaphone')
                    if query is None or query.empty:
                        logging.warning('There is no street around named or metaphone named: %s', data)
                        self.__street = data
                    else:
                        new_data = query.at[0, 'name']
                        logging.info('There is a metaphone street around named: %s, original was: %s.', new_data, data)
                        self.__street = new_data
                else:
                    logging.info('There is a street around named: %s.', data)
                    self.__street = data
            else:
                logging.debug('There are not coordinates. Is this a bug or missing data?')
                self.__street = data
        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')

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
                 self.__authentication_app, self.__authentication_membership_card, self.__capacity, self.__fee,
                 self.__parking_fee, self.__motorcar, self.__socket_chademo, self.__socket_chademo_output,
                 self.__socket_type2_combo, self.__socket_type2_combo_output,
                 self.__socket_type2_cable, self.__socket_type2_cable_output,
                 self.__socket_type2, self.__socket_type2_output, self.__manufacturer, self.__model,
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
                 self.__oh.at[WeekDaysShort.su, OpenClose.summer_close], self.__lunch_break_start,
                 self.__lunch_break_stop,
                 self.__public_holiday_open, self.__opening_hours, self.__lat, self.__lon, self.__good, self.__bad])
            self.clear_all()
        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')

    def process(self):
        df = pd.DataFrame(self.insert_data)
        df.columns = POI_COLS
        return df.where((pd.notnull(df)), None)
