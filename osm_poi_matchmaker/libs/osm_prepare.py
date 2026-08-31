# -*- coding: utf-8 -*-
try:
    import logging
    import sys
    import traceback
    import sqlalchemy
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def index_osm_data(session):
    """Create (if missing) every index the matcher's spatial/text queries rely on for
    reasonable performance against the osm2pgsql-imported planet_osm_point/line/polygon
    tables, plus street_type. Called once at STAGE 1 (see create_db.py).

    Safe to call repeatedly: every statement uses CREATE INDEX IF NOT EXISTS and
    the whole batch runs as one transaction (rolled back on any failure).

    Args:
        session: SQLAlchemy session.

    Returns:
        The CursorResult from executing the index-creation batch, or None if it
        failed (in which case the transaction was rolled back).
    """
    data = None
    try:
        query = sqlalchemy.text('''

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS i_street_type ON street_type(street_type);
-- osm2pgsql already creates planet_osm_point_way_idx as a proper spatial GIST index;
-- this one is a plain btree on a geometry column, which can't serve the ST_DistanceSphere
-- etc. spatial predicates every matching query uses - confirmed 0 idx_scan in production.
-- Dropped rather than "fixed" to gist, since the native index already covers that.
DROP INDEX IF EXISTS i_planet_osm_point_way;
CREATE INDEX IF NOT EXISTS i_planet_osm_point_amenity_addr_lower
    ON planet_osm_point("osm_id",LOWER("amenity"),LOWER("name"),LOWER("brand"),LOWER("addr:street"));
CREATE INDEX IF NOT EXISTS i_planet_osm_point_highway_addr_lower
    ON planet_osm_point("osm_id",LOWER("highway"),LOWER("name"));
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addr_lower
    ON planet_osm_point(LOWER("addr:postcode"),LOWER("addr:city"),LOWER("addr:street"),LOWER("addr:housenumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addr
    ON planet_osm_point("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addrcon_lower
    ON planet_osm_point(LOWER("addr:city"),LOWER("addr:conscriptionnumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addrcon ON planet_osm_point("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_point_amenity ON planet_osm_point(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_highway ON planet_osm_point(highway);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_shop ON planet_osm_point(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_name ON planet_osm_point(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_brand ON planet_osm_point(brand);
-- Plain btree indexes above don't help query_osm_shop_poi_gpd()'s name/brand/network
-- regex matching (btree only serves equality/prefix lookups); trigram GIN
-- indexes do. planet_osm_line already had one for name below - point/polygon didn't.
CREATE INDEX IF NOT EXISTS i_planet_osm_point_name_trgm ON planet_osm_point USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_brand_trgm ON planet_osm_point USING gin (brand gin_trgm_ops);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_network_trgm ON planet_osm_point USING gin (network gin_trgm_ops);


CREATE INDEX IF NOT EXISTS i_planet_osm_line_way ON planet_osm_line using gist(way);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_name_trgm ON planet_osm_line USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_brand_trgm ON planet_osm_line USING gin (brand gin_trgm_ops);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_network_trgm ON planet_osm_line USING gin (network gin_trgm_ops);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_amenity_addr_lower
    ON planet_osm_line("osm_id",LOWER("amenity"),LOWER("name"),LOWER("brand"),LOWER("addr:street"));
CREATE INDEX IF NOT EXISTS i_planet_osm_line_highway_addr_lower
    ON planet_osm_line("osm_id",LOWER("highway"),LOWER("name"));
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addr_lower
    ON planet_osm_line(LOWER("addr:postcode"),LOWER("addr:city"),LOWER("addr:street"),LOWER("addr:housenumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addr
    ON planet_osm_line("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addrcon_lower
    ON planet_osm_line(LOWER("addr:city"),LOWER("addr:conscriptionnumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addrcon ON planet_osm_line("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_line_amenity ON planet_osm_line(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_highway ON planet_osm_line(highway);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_shop ON planet_osm_line(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_name ON planet_osm_line(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_brand ON planet_osm_line(brand);


CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_way ON planet_osm_polygon using gist (way);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_amenity_addr_lower
    ON planet_osm_polygon("osm_id",LOWER("amenity"),LOWER("name"),LOWER("brand"),LOWER("addr:street"));
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_highway_addr_lower
    ON planet_osm_polygon("osm_id",LOWER("highway"),LOWER("name"));
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addr_lower
    ON planet_osm_polygon(LOWER("addr:postcode"),LOWER("addr:city"),LOWER("addr:street"),LOWER("addr:housenumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addr
    ON planet_osm_polygon("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addrcon_lower
    ON planet_osm_polygon(LOWER("addr:city"),LOWER("addr:conscriptionnumber"));
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addrcon ON planet_osm_polygon("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_amenity ON planet_osm_polygon(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_highway ON planet_osm_polygon(highway);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_shop ON planet_osm_polygon(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_name ON planet_osm_polygon(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_brand ON planet_osm_polygon(brand);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_name_trgm ON planet_osm_polygon USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_brand_trgm ON planet_osm_polygon USING gin (brand gin_trgm_ops);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_network_trgm ON planet_osm_polygon USING gin (network gin_trgm_ops);

-- Refresh planner statistics explicitly rather than relying on whatever CREATE INDEX
-- happens to update as a side effect - stale stats after a big osm2pgsql load can make
-- the planner drastically misjudge selectivity and pick a much slower plan even with
-- otherwise-suitable indexes in place.
ANALYZE planet_osm_point;
ANALYZE planet_osm_polygon;
ANALYZE planet_osm_line;
''')
        data = session.execute(query)
    except Exception as e:
        logging.exception('Exception occurred: {} rolled back: {}'.format(e, traceback.format_exc()))
        session.rollback()
    else:
        try:
            session.commit()
            logging.info('Successfully added database indexes.')
        except Exception as e:
            logging.exception('Exception occurred: {} unsuccessfully commit: {}'.format(e, traceback.format_exc()))
            session.rollback()
    finally:
        session.close()
    if data is None:
        return None
    else:
        return data
