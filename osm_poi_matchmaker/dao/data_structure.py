# -*- coding: utf-8 -*-

try:
    from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, UniqueConstraint
    from sqlalchemy import Integer, Unicode, DateTime, Enum, func
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import synonym, relationship
    from geoalchemy2 import Geometry
    import enum
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)

Base = declarative_base()


class OSM_type(enum.Enum):
    node = 0
    way = 1


class POI_address(Base):
    __tablename__ = 'poi_address'
    _plural_name_ = 'poi_address'
    pa_id = Column(Integer, primary_key=True, index=True)
    id = synonym('pa_id')
    poi_common_id = Column(ForeignKey('poi_common.pc_id'), index=True)
    poi_branch = Column(Unicode(64), nullable=True, index=True)
    poi_addr_city = Column(ForeignKey('city.city_id'), index=True)
    poi_postcode = Column(Integer)
    poi_city = Column(Unicode(64))
    poi_addr_street = Column(Unicode(64))
    poi_addr_housenumber = Column(Unicode(16))
    poi_conscriptionnumber = Column(Unicode(16))
    poi_geom = Column(Geometry('POINT'))
    original = Column(Unicode(128))
    poi_website = Column(Unicode(256))
    poi_ref = Column(Unicode(16))
    poi_opening_hours_mo = Column(Unicode(64))
    poi_opening_hours_tu = Column(Unicode(64))
    poi_opening_hours_we = Column(Unicode(64))
    poi_opening_hours_th = Column(Unicode(64))
    poi_opening_hours_fr = Column(Unicode(64))
    poi_opening_hours_sa = Column(Unicode(64))
    poi_opening_hours_su = Column(Unicode(64))
    poi_hash = Column(Unicode(128), nullable=True, unique=False, index=True)
    poi_created = Column(DateTime(True), nullable=False, server_default=func.now())
    poi_updated = Column(DateTime(True))
    poi_deleted = Column(DateTime(True))

    common = relationship('POI_common', primaryjoin='POI_address.poi_common_id == POI_common.pc_id',
                          backref='poi_address')
    city = relationship('City', primaryjoin='POI_address.poi_addr_city == City.city_id', backref='poi_address')

    def __repr__(self):
        return '<POI address {}: {}>'.format(self.pa_id, self.poi_name)


class POI_common(Base):
    __tablename__ = 'poi_common'
    _plural_name_ = 'poi_common'
    pc_id = Column(Integer, primary_key=True, index=True)
    id = synonym('pc_id')
    poi_name = Column(Unicode(64), unique=False, nullable=False, index=True)
    poi_tags = Column(Unicode(1024), nullable=False, index=True)
    poi_url_base = Column(Unicode(32))
    poi_code = Column(Unicode(10), unique=True, nullable=False, index=True)

    def __repr__(self):
        return '<POI common {}: {}>'.format(self.pc_id, self.poi_name)


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


class POI_osm(Base):
    __tablename__ = 'poi_osm'
    _plural_name_ = 'poi_osm'
    po_id = Column(Integer, primary_key=True, index=True)
    id = synonym('po_id')
    poi_osm_id = Column(Integer, unique=True, index=True)
    poi_osm_type = Column(Enum(OSM_type))
    poi_hash = Column(Unicode(128), nullable=True, unique=False, index=True)
    geom_hint = Column(Geometry('POINT'))

    __table_args__ = (UniqueConstraint('poi_osm_id', 'poi_osm_type', name='uc_poi_osm_osm_type'),)
