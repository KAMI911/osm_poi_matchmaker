# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, UniqueConstraint
    from sqlalchemy import Boolean, Integer, BigInteger, Unicode, DateTime, Time, Enum, Float, func
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import synonym, relationship
    from geoalchemy2 import Geometry
    import enum
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)

Base = declarative_base()


class OSM_object_type(enum.Enum):
    node = 0
    way = 1
    relation = 2


class POI_type(enum.Enum):
    shop = 0
    fuel = 1
    bank = 2
    atm = 3
    post_office = 4
    vending_machine = 5
    pharmacy = 6
    chemist = 7
    bicycle_rental = 8
    vending_machine_cheques = 9
    vending_machine_parcel_pickup = 10
    vending_machine_parcel_mail_in = 11
    vending_machine_parcel_pickup_and_mail_in = 12
    vending_machine_parking_tickets = 13
    tobacco = 14
    clothes = 15
    doityourself = 16


class POI_address(Base):
    __tablename__ = 'poi_address'
    _plural_name_ = 'poi_address'
    pa_id = Column(Integer, primary_key=True, index=True)
    id = synonym('pa_id')
    poi_common_id = Column(ForeignKey('poi_common.pc_id'), index=True)
    poi_branch = Column(Unicode(128), nullable=True, index=True)
    poi_addr_city = Column(ForeignKey('city.city_id'), index=True)
    poi_postcode = Column(Integer)
    poi_city = Column(Unicode(64))
    poi_addr_street = Column(Unicode(128))
    poi_addr_housenumber = Column(Unicode(16))
    poi_conscriptionnumber = Column(Unicode(16))
    poi_geom = Column(Geometry('POINT, {}'.format(config.get_geo_default_projection())))
    original = Column(Unicode(128))
    poi_website = Column(Unicode(256))
    poi_description = Column(Unicode(1024))
    poi_fuel_adblue = Column(Boolean)
    poi_fuel_octane_100 = Column(Boolean)
    poi_fuel_octane_98 = Column(Boolean)
    poi_fuel_octane_95 = Column(Boolean)
    poi_fuel_diesel_gtl = Column(Boolean)
    poi_fuel_diesel = Column(Boolean)
    poi_fuel_lpg = Column(Boolean)
    poi_fuel_e85 = Column(Boolean)
    poi_rent_lpg_bottles = Column(Boolean)
    poi_compressed_air = Column(Boolean)
    poi_restaurant = Column(Boolean)
    poi_food = Column(Boolean)
    poi_truck = Column(Boolean)
    poi_ref = Column(Unicode(16))
    poi_phone = Column(Unicode(64))
    poi_email = Column(Unicode(64))
    poi_opening_hours_nonstop = Column(Boolean)
    poi_opening_hours_mo_open = Column(Time)
    poi_opening_hours_tu_open = Column(Time)
    poi_opening_hours_we_open = Column(Time)
    poi_opening_hours_th_open = Column(Time)
    poi_opening_hours_fr_open = Column(Time)
    poi_opening_hours_sa_open = Column(Time)
    poi_opening_hours_su_open = Column(Time)
    poi_opening_hours_mo_close = Column(Time)
    poi_opening_hours_tu_close = Column(Time)
    poi_opening_hours_we_close = Column(Time)
    poi_opening_hours_th_close = Column(Time)
    poi_opening_hours_fr_close = Column(Time)
    poi_opening_hours_sa_close = Column(Time)
    poi_opening_hours_su_close = Column(Time)
    poi_opening_hours_summer_mo_open = Column(Time)
    poi_opening_hours_summer_tu_open = Column(Time)
    poi_opening_hours_summer_we_open = Column(Time)
    poi_opening_hours_summer_th_open = Column(Time)
    poi_opening_hours_summer_fr_open = Column(Time)
    poi_opening_hours_summer_sa_open = Column(Time)
    poi_opening_hours_summer_su_open = Column(Time)
    poi_opening_hours_summer_mo_close = Column(Time)
    poi_opening_hours_summer_tu_close = Column(Time)
    poi_opening_hours_summer_we_close = Column(Time)
    poi_opening_hours_summer_th_close = Column(Time)
    poi_opening_hours_summer_fr_close = Column(Time)
    poi_opening_hours_summer_sa_close = Column(Time)
    poi_opening_hours_summer_su_close = Column(Time)
    poi_opening_hours_lunch_break_start = Column(Time)
    poi_opening_hours_lunch_break_stop = Column(Time)
    poi_public_holiday_open = Column(Boolean)
    poi_opening_hours = Column(Unicode(256), nullable=True, unique=False, index=True)
    poi_good = Column(Unicode(128), nullable=True, index=False)
    poi_bad = Column(Unicode(128), nullable=True, index=False)
    poi_hash = Column(Unicode(128), nullable=True, unique=False, index=True)
    poi_created = Column(DateTime(True), nullable=False, server_default=func.now())
    poi_updated = Column(DateTime(True))
    poi_deleted = Column(DateTime(True))

    common = relationship('POI_common', primaryjoin='POI_address.poi_common_id == POI_common.pc_id',
                          backref='poi_address')
    city = relationship('City', primaryjoin='POI_address.poi_addr_city == City.city_id', backref='poi_address')

    # def __repr__(self):
    #  return '<POI address {}: {}>'.format(self.pa_id, self.poi_name)


class POI_common(Base):
    __tablename__ = 'poi_common'
    _plural_name_ = 'poi_common'
    pc_id = Column(Integer, primary_key=True, index=True)
    id = synonym('pc_id')
    poi_name = Column(Unicode(64), unique=False, nullable=False, index=True)
    poi_type = Column(Enum(POI_type))
    poi_tags = Column(Unicode(2048), nullable=False, index=True)
    poi_url_base = Column(Unicode(32))
    poi_code = Column(Unicode(10), unique=True, nullable=False, index=True)
    poi_search_name = Column(Unicode(64))
    preserve_original_name = Column(Boolean,  nullable=False, default=False)
    osm_search_distance_perfect = Column(Integer, nullable=True, index=False)
    osm_search_distance_safe = Column(Integer, nullable=True, index=False)
    osm_search_distance_unsafe = Column(Integer, nullable=True, index=False)


    def __repr__(self):
        return '<POI common {}: {}>'.format(self.pc_id, self.poi_name)


class POI_OSM_cache(Base):
    __tablename__ = 'poi_osm_cache'
    _plural_name_ = 'poi_osm_cache'
    poc_id = Column(Integer, primary_key=True, index=True)
    id = synonym('poc_id')
    #poi_type = Column(Enum(POI_type))
    osm_id = Column(BigInteger, nullable=False, index=True)
    osm_object_type = Column(Enum(OSM_object_type))
    osm_version = Column(Integer, nullable=False, index=True)
    osm_user = Column(Unicode(64), nullable=True, index=False)
    osm_user_id = Column(Integer, nullable=False, index=True)
    osm_changeset = Column(Integer, nullable=False, index=True)
    osm_timestamp = Column(DateTime(True), nullable=False)
    osm_lat = Column(Float, nullable=True, index=True)
    osm_lon = Column(Float, nullable=True, index=True)
    osm_nodes = Column(Unicode(1024), nullable=True, index=False)
    #osm_distance = Column(Integer, nullable=True, index=False)
    osm_live_tags = Column(Unicode(2048), nullable=True, index=True)


class City(Base):
    __tablename__ = 'city'
    _plural_name_ = 'city'
    city_id = Column(Integer, primary_key=True, index=True)
    id = synonym('city_id')
    city_name = Column(Unicode)
    city_post_code = Column(Integer)

    __table_args__ = (UniqueConstraint('city_name', 'city_post_code', name='uc_city_name_post_code'),)

    def __repr__(self):
        return '<City {}: {} ({})>'.format(self.city_id, self.city_name, self.city_post_code)


class Street_type(Base):
    __tablename__ = 'street_type'
    _plural_name_ = 'street_type'
    st_id = Column(Integer, primary_key=True, index=True)
    id = synonym('st_id')
    street_type = Column(Unicode(20))

    def __repr__(self):
        return '<Street type {}: {}>'.format(self.st_id, self.street_type)


class POI_osm(Base):
    __tablename__ = 'poi_osm'
    _plural_name_ = 'poi_osm'
    po_id = Column(Integer, primary_key=True, index=True)
    id = synonym('po_id')
    poi_osm_id = Column(BigInteger, unique=True, index=True)
    poi_osm_object_type = Column(Enum(OSM_object_type))
    poi_hash = Column(Unicode(128), nullable=True, unique=False, index=True)
    geom_hint = Column(Geometry('POINT, {}'.format(config.get_geo_default_projection())))

    __table_args__ = (UniqueConstraint('poi_osm_id', 'poi_osm_object_type', name='uc_poi_osm_osm_type'),)
