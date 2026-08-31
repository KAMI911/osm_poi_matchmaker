# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, UniqueConstraint
    from sqlalchemy import Boolean, Integer, BigInteger, Unicode, DateTime, Time, Float, JSON, func
    from sqlalchemy import Enum as SAEnum
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import synonym, relationship
    from geoalchemy2 import Geometry
    import enum
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

Base = declarative_base()


class OSM_object_type(enum.Enum):
    """OSM element kind a POI was matched to (or created as): node, way or relation."""
    node = 0
    way = 1
    relation = 2


class POI_type(enum.Enum):
    """Supported POI categories; each value corresponds to a ptype string accepted
    by utils/poitypes.py's getPOITypes()."""
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
    vending_machine_parcel_locker = 10
    vending_machine_parcel_mail_in = 11
    vending_machine_parcel_locker_and_mail_in = 12
    vending_machine_parking_tickets = 13
    tobacco = 14
    clothes = 15
    doityourself = 16
    cosmetics = 17
    furniture = 18
    charging_station = 19
    waterway_fuel = 20
    fastfood = 21
    shoes = 22
    optician = 23
    bus_stop = 24
    railway_station = 25
    fire_station = 26
    post_partner = 27


class POI_address_raw(Base):
    """Staging table: one row per POI as harvested from a data provider, before it has
    been matched against (or merged with) any existing OSM data. Populated by
    dao/data_handlers.py's insert_poi_dataframe() during STAGE 2 (POI harvesting)."""
    __tablename__ = 'poi_address_raw'
    _plural_name_ = 'poi_address_raw'
    pa_id = Column(Integer, primary_key=True, index=True)
    id = synonym('pa_id')
    poi_common_id = Column(ForeignKey('poi_common.pc_id'), index=True)
    poi_name = Column(Unicode(128), unique=False, nullable=True, index=True)
    poi_branch = Column(Unicode(128), nullable=True, index=True)
    poi_addr_city = Column(ForeignKey('city.city_id'), index=True)
    poi_postcode = Column(Unicode(12))
    poi_city = Column(Unicode(64))
    poi_addr_street = Column(Unicode(128))
    poi_addr_housenumber = Column(Unicode(16))
    poi_conscriptionnumber = Column(Unicode(16))
    poi_geom = Column(Geometry('POINT, {}'.format(config.get_geo_default_projection())))
    original = Column(Unicode(512))
    poi_website = Column(Unicode(256))
    poi_description = Column(Unicode(2048))
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
    poi_ref = Column(Unicode(64))
    poi_additional_ref = Column(Unicode(64))
    poi_phone = Column(Unicode(64))
    poi_mobile = Column(Unicode(64))
    poi_email = Column(Unicode(128))
    poi_authentication_app = Column(Boolean)
    poi_authentication_none = Column(Boolean)
    poi_authentication_membership_card = Column(Boolean)
    poi_capacity = Column(Integer)
    poi_fee = Column(Boolean)
    poi_parking_fee = Column(Boolean)
    poi_motorcar = Column(Boolean)
    poi_socket_chademo = Column(Integer)
    poi_socket_chademo_output = Column(Unicode(16))
    poi_socket_chademo_current = Column(Integer)
    poi_socket_chademo_voltage = Column(Integer)
    poi_socket_type2_combo = Column(Integer)
    poi_socket_type2_combo_output = Column(Unicode(16))
    poi_socket_type2_combo_current = Column(Integer)
    poi_socket_type2_combo_voltage = Column(Integer)
    poi_socket_type2_cable = Column(Integer)
    poi_socket_type2_cable_output = Column(Unicode(16))
    poi_socket_type2_cable_current = Column(Integer)
    poi_socket_type2_cable_voltage = Column(Integer)
    poi_socket_type2_cableless = Column(Integer)
    poi_socket_type2_cableless_output = Column(Unicode(16))
    poi_socket_type2_cableless_current = Column(Integer)
    poi_socket_type2_cableless_voltage = Column(Integer)
    poi_manufacturer = Column(Unicode(32))
    poi_model = Column(Unicode(32))
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
    poi_lat = Column(Float, nullable=True, index=True)
    poi_lon = Column(Float, nullable=True, index=True)
    poi_created = Column(DateTime(True), nullable=False, server_default=func.now())
    poi_updated = Column(DateTime(True))
    poi_deleted = Column(DateTime(True))
    # GTFS conflation extras (e.g. Volanbusz's parent_station/location_type stop hierarchy)
    # written as gtfs:parent_station:<feed>/gtfs:location_type:<feed> tags - see POI_common's
    # gtfs_feed_id, which supplies the <feed> part. Declared last to match the physical column
    # order the live DB already has these appended in (ALTER TABLE ADD COLUMN always appends).
    poi_gtfs_parent_station = Column(Unicode(64))
    poi_gtfs_location_type = Column(Unicode(4))
    # Generic (not GTFS-specific) OSM reference tags a provider may be able to source
    # externally, e.g. hu_mav.py matching its GTFS stops against Wikidata by name/
    # coordinate to find a UIC station code and the station's Wikidata item.
    poi_uic_ref = Column(Unicode(16))
    poi_wikidata = Column(Unicode(16))

    common = relationship('POI_common', primaryjoin='POI_address_raw.poi_common_id == POI_common.pc_id',
                          backref='poi_address_raw')
    city = relationship('City', primaryjoin='POI_address_raw.poi_addr_city == City.city_id', backref='poi_address_raw')

    def __repr__(self):
        return '<POI Address RAW {}: {} {} {} {};{}>'.format(
            self.id,
            self.poi_city,
            self.poi_addr_street,
            self.poi_addr_housenumber,
            self.poi_lat,
            self.poi_lon
        )


class POI_address(Base):
    """Same shape as POI_address_raw, plus OSM enrichment columns (osm_id, osm_node,
    osm_version, osm_live_tags, poi_good/poi_bad, ...) added once the online matcher
    (STAGE 8) has run. This is the table the final OSM XML/GeoJSON export reads from."""
    __tablename__ = 'poi_address'
    _plural_name_ = 'poi_address'
    pa_id = Column(Integer, primary_key=True, index=True)
    id = synonym('pa_id')
    poi_common_id = Column(ForeignKey('poi_common.pc_id'), index=True)
    poi_name = Column(Unicode(128), unique=False, nullable=True, index=True)
    poi_branch = Column(Unicode(128), nullable=True, index=True)
    poi_addr_city = Column(ForeignKey('city.city_id'), index=True)
    poi_postcode = Column(Unicode(12))
    poi_city = Column(Unicode(64))
    poi_addr_street = Column(Unicode(128))
    poi_addr_housenumber = Column(Unicode(16))
    poi_conscriptionnumber = Column(Unicode(16))
    poi_geom = Column(Geometry('POINT, {}'.format(config.get_geo_default_projection())))
    original = Column(Unicode(512))
    poi_website = Column(Unicode(256))
    poi_description = Column(Unicode(2048))
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
    poi_ref = Column(Unicode(64))
    poi_additional_ref = Column(Unicode(64))
    poi_phone = Column(Unicode(64))
    poi_mobile = Column(Unicode(64))
    poi_email = Column(Unicode(128))
    poi_authentication_app = Column(Boolean)
    poi_authentication_none = Column(Boolean)
    poi_authentication_membership_card = Column(Boolean)
    poi_capacity = Column(Integer)
    poi_fee = Column(Boolean)
    poi_parking_fee = Column(Boolean)
    poi_motorcar = Column(Boolean)
    poi_socket_chademo = Column(Integer)
    poi_socket_chademo_output = Column(Unicode(16))
    poi_socket_chademo_current = Column(Integer)
    poi_socket_chademo_voltage = Column(Integer)
    poi_socket_type2_combo = Column(Integer)
    poi_socket_type2_combo_output = Column(Unicode(16))
    poi_socket_type2_combo_current = Column(Integer)
    poi_socket_type2_combo_voltage = Column(Integer)
    poi_socket_type2_cable = Column(Integer)
    poi_socket_type2_cable_output = Column(Unicode(16))
    poi_socket_type2_cable_current = Column(Integer)
    poi_socket_type2_cable_voltage = Column(Integer)
    poi_socket_type2_cableless = Column(Integer)
    poi_socket_type2_cableless_output = Column(Unicode(16))
    poi_socket_type2_cableless_current = Column(Integer)
    poi_socket_type2_cableless_voltage = Column(Integer)
    poi_manufacturer = Column(Unicode(32))
    poi_model = Column(Unicode(32))
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
    osm_changeset = Column(Integer)
    osm_search_distance_unsafe = Column(Integer)
    osm_search_distance_safe = Column(Integer)
    osm_search_distance_perfect = Column(Integer)
    osm_id = Column(BigInteger, nullable=True, index=True)
    osm_timestamp = Column(DateTime(True))
    poi_tags = Column(JSON, nullable=True, unique=False)
    osm_live_tags = Column(JSON, nullable=True, unique=False)
    poi_type = Column(Unicode(64))
    osm_version = Column(Integer)
    osm_node = Column(SAEnum(OSM_object_type, name="osm_object_type"), nullable=True)
    osm_nodes = Column(JSON, nullable=True, index=False)
    poi_url_base = Column(Unicode(128))
    preserve_original_name = Column(Boolean)
    preserve_original_post_code = Column(Boolean)
    poi_search_name = Column(Unicode(128))
    poi_search_avoid_name = Column(Unicode(128))
    poi_lat = Column(Float, nullable=True, index=True)
    poi_lon = Column(Float, nullable=True, index=True)
    poi_good = Column(JSON, nullable=True, index=False)
    poi_bad = Column(JSON, nullable=True, index=False)
    poi_hash = Column(Unicode(128), nullable=True, unique=False, index=True)
    poi_created = Column(DateTime(True), nullable=False, server_default=func.now())
    poi_updated = Column(DateTime(True))
    poi_deleted = Column(DateTime(True))
    poi_gtfs_parent_station = Column(Unicode(64))
    poi_gtfs_location_type = Column(Unicode(4))
    poi_uic_ref = Column(Unicode(16))
    poi_wikidata = Column(Unicode(16))
    common = relationship('POI_common', primaryjoin='POI_address.poi_common_id == POI_common.pc_id',
                          backref='poi_address')
    city = relationship('City', primaryjoin='POI_address.poi_addr_city == City.city_id', backref='poi_address')

    # 'osm_changeset', 'osm_id', 'osm_live_tags', 'osm_node', 'osm_search_distance_perfect', 'osm_search_distance_safe', 'osm_search_distance_unsafe', 'osm_timestamp', 'osm_version', 'poi_search_avoid_name', 'poi_search_name', 'poi_tags', 'poi_type', 'poi_url_base', 'preserve_original_name', 'preserve_original_post_code'
    # def __repr__(self):
    #  return '<POI address {}: {}>'.format(self.pa_id, self.poi_name)

    def __init__(self, **entries):
        '''Override to avoid TypeError when passed spurious column names'''
        col_names = set([col.name for col in self.__table__.columns])
        superentries = {k: entries[k] for k in col_names.intersection(entries.keys())}
        super().__init__(**superentries)

    def __repr__(self):
        return '<POI Address {}: {} {} {} {};{}>'.format(
            self.id,
            self.poi_city,
            self.poi_addr_street,
            self.poi_addr_housenumber,
            self.poi_lat,
            self.poi_lon
        )


class POI_common(Base):
    """One row per POI type a provider declares in its types() (e.g. 'hualdisup' for
    Aldi supermarkets): the shared OSM tags, search name/distance thresholds and
    other settings common to every POI of that type. Referenced by
    POI_address(_raw).poi_common_id."""
    __tablename__ = 'poi_common'
    _plural_name_ = 'poi_common'
    pc_id = Column(Integer, primary_key=True, index=True)
    id = synonym('pc_id')
    poi_common_name = Column(Unicode(64), unique=False, nullable=False, index=True)
    poi_type = Column(SAEnum(POI_type))
    poi_tags = Column(JSON, nullable=False, index=False)
    poi_url_base = Column(Unicode(128))
    poi_code = Column(Unicode(10), unique=True, nullable=False, index=True)
    poi_search_name = Column(Unicode(128))
    poi_search_avoid_name = Column(Unicode(128))
    preserve_original_name = Column(Boolean, nullable=False, default=False)
    preserve_original_post_code = Column(Boolean, nullable=False, default=False)
    export_poi_name = Column(Boolean, nullable=False, default=True)
    do_not_export_addr_tags = Column(Boolean, nullable=False, default=False)
    osm_search_distance_perfect = Column(Integer, nullable=True, index=False)
    osm_search_distance_safe = Column(Integer, nullable=True, index=False)
    osm_search_distance_unsafe = Column(Integer, nullable=True, index=False)
    additional_ref_name = Column(Unicode(32), nullable=True, index=False)
    # GTFS feed id (e.g. 'HU-VOLAN') used to compose gtfs:parent_station:<feed> and
    # gtfs:location_type:<feed> tags from poi_gtfs_parent_station/poi_gtfs_location_type
    # (POI_address_raw) at export time - see file_output.py.
    gtfs_feed_id = Column(Unicode(16), nullable=True, index=False)

    def __repr__(self):
        return '<POI common {}: {}>'.format(self.pc_id, self.poi_common_name)


class POI_OSM_cache(Base):
    """Cache of OSM live-tag lookups (osmapi node_get/way_get/relation_get results),
    keyed by osm_id, so the matcher doesn't re-fetch the same element's live tags on
    every run. See libs/online_poi_matching.py and dao/data_handlers.get_or_create_cache()."""
    __tablename__ = 'poi_osm_cache'
    _plural_name_ = 'poi_osm_cache'
    poc_id = Column(Integer, primary_key=True, index=True)
    id = synonym('poc_id')
    # poi_type = Column(SAEnum(POI_type))
    osm_id = Column(BigInteger, nullable=False, index=True)
    osm_object_type = Column(SAEnum(OSM_object_type))
    osm_version = Column(Integer, nullable=False, index=True)
    osm_user = Column(Unicode(64), nullable=True, index=False)
    osm_user_id = Column(Integer, nullable=True, index=True)
    osm_changeset = Column(Integer, nullable=False, index=True)
    osm_timestamp = Column(DateTime(True), nullable=False)
    osm_lat = Column(Float, nullable=True, index=True)
    osm_lon = Column(Float, nullable=True, index=True)
    osm_nodes = Column(JSON, nullable=True, index=False)
    # osm_distance = Column(Integer, nullable=True, index=False)
    osm_live_tags = Column(JSON, nullable=True, index=False)


class City(Base):
    """Lookup table of Hungarian city names and their postcode(s), imported from
    Magyar Posta's ZipCodes.xml (see dataproviders/hu_generic.py). Used to validate/
    normalize city names during address processing."""
    __tablename__ = 'city'
    _plural_name_ = 'city'
    city_id = Column(Integer, primary_key=True, index=True)
    id = synonym('city_id')
    city_name = Column(Unicode(128))
    city_post_code = Column(Unicode(12))

    __table_args__ = (UniqueConstraint('city_name', 'city_post_code', name='uc_city_name_post_code'),)

    def __repr__(self):
        return '<City {}: {} ({})>'.format(self.city_id, self.city_name, self.city_post_code)


class Street_type(Base):
    """Lookup table of Hungarian street-type suffixes (utca, út, tér, körút, ...),
    imported from Magyar Posta's StreetTypes.xml, used by libs/address.py's
    clean_street_type() to normalize/validate street type words."""
    __tablename__ = 'street_type'
    _plural_name_ = 'street_type'
    st_id = Column(Integer, primary_key=True, index=True)
    id = synonym('st_id')
    street_type = Column(Unicode(20))

    def __repr__(self):
        return '<Street type {}: {}>'.format(self.st_id, self.street_type)


class POI_osm(Base):
    """Table for hash+geometry hints per OSM element (poi_osm_id/poi_osm_object_type,
    poi_hash, geom_hint). Currently only created/dropped alongside the other POI
    tables (see import_poi_data_module.py); nothing in this codebase writes to or
    queries it yet."""
    __tablename__ = 'poi_osm'
    _plural_name_ = 'poi_osm'
    po_id = Column(Integer, primary_key=True, index=True)
    id = synonym('po_id')
    poi_osm_id = Column(BigInteger, unique=True, index=True)
    poi_osm_object_type = Column(SAEnum(OSM_object_type))
    poi_hash = Column(Unicode(128), nullable=True, unique=False, index=True)
    geom_hint = Column(Geometry(geometry_type='GEOMETRY', srid=config.get_geo_default_projection()))

    __table_args__ = (UniqueConstraint('poi_osm_id', 'poi_osm_object_type', name='uc_poi_osm_osm_type'),)


class POI_patch(Base):
    """Manual address corrections keyed by poi_code: if a POI's harvested address
    (orig_*) matches a row here, it gets overwritten with the corrected address
    (new_*) before matching. Loaded from poi_patch.tsv via
    dataproviders/hu_generic.py's poi_patch_from_csv and applied in
    libs/poi_patch.py."""
    __tablename__ = 'poi_patch'
    _plural_name_ = 'poi_patch'
    ph_id = Column(Integer, primary_key=True, index=True)
    id = synonym('ph_id')
    poi_code = Column(Unicode(10), unique=False, nullable=True, index=True)
    orig_postcode = Column(Unicode(12))
    orig_city = Column(Unicode(64))
    orig_street = Column(Unicode(128))
    orig_housenumber = Column(Unicode(16))
    orig_conscriptionnumber = Column(Unicode(16))
    orig_name = Column(Unicode(64), unique=False, nullable=True, index=True)
    new_postcode = Column(Unicode(12))
    new_city = Column(Unicode(64))
    new_street = Column(Unicode(128))
    new_housenumber = Column(Unicode(16))
    new_conscriptionnumber = Column(Unicode(16))
    new_name = Column(Unicode(64), unique=False, nullable=True, index=True)

    def __repr__(self):
        return '<POI patch {} {} {} {}>'.format(self.ph_id, self.orig_city, self.orig_street, self.orig_housenumber)


class Country(Base):
    """Reference table of countries (ISO code, name, continent, etc.), imported from
    country.tsv via dataproviders/hu_generic.py's poi_country_from_csv."""
    __tablename__ = 'country'
    _plural_name_ = 'country'
    cy_id = Column(Integer, primary_key=True, index=True)
    id = synonym('cy_id')
    country_code = Column(Unicode(2), unique=True, nullable=False, index=True)
    continent_code = Column(Unicode(2), unique=False, nullable=True, index=True)
    country_name = Column(Unicode(64), unique=True, nullable=False, index=True)
    country_iso3 = Column(Unicode(3), unique=True, nullable=False, index=True)
    country_number = Column(Integer, unique=True, nullable=False, index=True)
    country_full_name = Column(Unicode(128), unique=True, nullable=False, index=True)

    def __repr__(self):
        return '<Country {}>'.format(self.country_code)
