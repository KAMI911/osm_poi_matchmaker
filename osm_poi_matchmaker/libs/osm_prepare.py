# -*- coding: utf-8 -*-
try:
    import logging
    import sys
    import sqlalchemy
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

def index_osm_data(session):
    query = sqlalchemy.text('''
CREATE INDEX IF NOT EXISTS i_street_type ON street_type(street_type);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_way ON planet_osm_point(way);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_amenity_addr_lower ON planet_osm_polygon(LOWER("amenity"),"osm_id",LOWER("name"),LOWER("brand"),LOWER("addr:street"));
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addr_lower ON planet_osm_point(LOWER("addr:postcode"),LOWER("addr:city"),LOWER("addr:street"),LOWER("addr:housenumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addr ON planet_osm_point("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addrcon_lower ON planet_osm_point(LOWER("addr:city"),LOWER("addr:conscriptionnumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addrcon ON planet_osm_point("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_point_amenity ON planet_osm_point(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_shop ON planet_osm_point(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_name ON planet_osm_point(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_brand ON planet_osm_point(brand);


CREATE INDEX IF NOT EXISTS i_planet_osm_line_way ON planet_osm_line using gist(way);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_amenity_addr_lower ON planet_osm_line(LOWER("amenity"),"osm_id",LOWER("name"),LOWER("brand"),LOWER("addr:street"));
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addr_lower ON planet_osm_line(LOWER("addr:postcode"),LOWER("addr:city"),LOWER("addr:street"),LOWER("addr:housenumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addr ON planet_osm_line("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addrcon_lower ON planet_osm_line(LOWER("addr:city"),LOWER("addr:conscriptionnumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addrcon ON planet_osm_line("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_line_amenity ON planet_osm_line(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_shop ON planet_osm_line(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_name ON planet_osm_line(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_brand ON planet_osm_line(brand);


CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_way ON planet_osm_polygon using gist (way);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_amenity_addr_lower ON planet_osm_polygon(LOWER("amenity"),"osm_id",LOWER("name"),LOWER("brand"),LOWER("addr:street"));
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addr_lower ON planet_osm_polygon(LOWER("addr:postcode"),LOWER("addr:city"),LOWER("addr:street"),LOWER("addr:housenumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addr ON planet_osm_polygon("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addrcon_lower ON planet_osm_polygon(LOWER("addr:city"),LOWER("addr:conscriptionnumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addrcon ON planet_osm_polygon("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_amenity ON planet_osm_polygon(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_shop ON planet_osm_polygon(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_name ON planet_osm_polygon(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_brand ON planet_osm_polygon(brand);
''')
    data = session.execute(query)
    if data is None:
        return None
    else:
        return data
