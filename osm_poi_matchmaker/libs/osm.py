# -*- coding: utf-8 -*-
try:
    import logging
    import sys
    import sqlalchemy
    import geopandas as gpd
    import datetime
    from OSMPythonTools.overpass import Overpass
    from OSMPythonTools.nominatim import Nominatim
    from OSMPythonTools.overpass import overpassQueryBuilder
    from osm_poi_matchmaker.libs.address import clean_string, clean_postcode
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def get_area_id(area):
    """Resolve a place name to its Overpass area id via Nominatim.

    Not called anywhere in this codebase currently (the matcher queries the local
    osm2pgsql database directly instead of the public Overpass/Nominatim APIs).

    Args:
        area (str): Place name to search for, e.g. 'Hungary'.

    Returns:
        The Overpass area id for the first Nominatim match.
    """
    # Query Nominatom
    nominatim = Nominatim()
    return nominatim.query(area).areaId()


def query_overpass(area_id, query_statement, element_type='node'):
    """Run an Overpass API query scoped to an area. See get_area_id() (unused
    alongside this function).

    Args:
        area_id: Overpass area id, e.g. from get_area_id().
        query_statement: Overpass QL selector, e.g. "'shop'='supermarket'".
        element_type (str): OSM element type to search for ('node', 'way', 'rel').

    Returns:
        The Overpass query result object.
    """
    # Query Overpass based on area
    overpass = Overpass()
    query = overpassQueryBuilder(area=area_id, elementType=element_type, selector=query_statement)
    return overpass.query(query)


def query_osm_postcode_gpd(session, lon, lat):
    """Look up the postal code of the OSM boundary=postal_code polygon containing a
    point, from the local osm2pgsql-imported database.

    Args:
        session: SQLAlchemy session (raw connection, not the ORM).
        lon: Point longitude.
        lat: Point latitude.

    Returns:
        int | None: The leading numeric part of the matching polygon's name (e.g.
        1011 from '1011 Budapest'), or None if lon/lat are missing or nothing matched.
    """
    if lat is None or lat == '' or lon == '' or lon is None:
        return None
    query = sqlalchemy.text('''
        SELECT name
        FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon, :lat),4326) as geom) point
        WHERE boundary='postal_code' and ST_Contains(way, point.geom) ORDER BY name LIMIT 1;''')
    data = None
    try:
        data = session.execute(query, {'lon': float(lon), 'lat': float(lat)}).first()
    except Exception as e:
        logging.error(e)
        logging.exception('Exception occurred')
    finally:
        session.commit()
    if data is None:
        return None
    row = dict(data._mapping)
    return int(row['name'].split(' ')[0]) if row['name'].split(' ')[0] is not None else None


def query_postcode_osm_external(prefer_osm, prefer_original, session, lon, lat, postcode_ext, postcode_original):
    """Decide which postcode to use for a POI, choosing between the data provider's
    value, OSM's own boundary polygon at that point, and the POI's existing OSM
    postcode - in an order controlled by the two preference flags.

    Args:
        prefer_osm (bool): If True, prefer the OSM boundary polygon's postcode over
            postcode_ext when both are available.
        prefer_original (bool): If True and postcode_original is set, return it
            immediately without querying OSM at all.
        session: SQLAlchemy session, passed through to query_osm_postcode_gpd().
        lon: POI longitude, used for the OSM boundary lookup.
        lat: POI latitude, used for the OSM boundary lookup.
        postcode_ext: Postcode from the data provider.
        postcode_original: The POI's existing (already-in-OSM) postcode, if any.

    Returns:
        str | None: The chosen postcode (cleaned via clean_postcode()), or None if no
        candidate matched any of the conditions below.
    """
    if prefer_original is True and clean_postcode(postcode_original) is not None:
        return clean_postcode(postcode_original)
    if prefer_osm is False and clean_postcode(postcode_ext) is not None:
        return clean_postcode(postcode_ext)
    query_postcode = query_osm_postcode_gpd(session, lon, lat)
    if prefer_original is True and clean_postcode(postcode_original) is None:
        if prefer_osm is True and clean_postcode(query_postcode) is not None:
            return clean_postcode(query_postcode)
        elif prefer_osm is True and clean_postcode(query_postcode) is None:
            return clean_postcode(postcode_ext)
    if prefer_osm is True and clean_postcode(query_postcode) is not None:
        return clean_postcode(query_postcode)
    elif prefer_osm is True and clean_postcode(query_postcode) is None:
        return clean_postcode(postcode_ext)


def relationer(relation_text):
    """Parse osm2pgsql's flat relation member array into structured member dicts.

    Args:
        relation_text: Flat list alternating [member_ref (e.g. 'n123', 'w456'),
            role, member_ref, role, ...], as stored in planet_osm_rels.members.

    Returns:
        list[dict] | None: One {'type': 'node'|'way'|'relation'|'unknown', 'ref': str,
        'role': str} dict per member, or None if relation_text is None.
    """
    if relation_text is None:
        return None
    data = []
    logging.info(f'Relation processing with this data: {relation_text}')

    for i in range(0, len(relation_text) - 1, 2):
        item = relation_text[i]
        tp = {'n': 'node', 'w': 'way', 'r': 'relation'}.get(item[0], 'unknown')
        rf = item[1:]
        rl = relation_text[i + 1]
        data.append({'type': tp, 'ref': rf, 'role': rl})
    return data


def timestamp_now():
    """Return the current local datetime (plain datetime.datetime.now(), no timezone
    handling)."""
    return datetime.datetime.now()


def osm_timestamp_now():
    """Return the current local time formatted as an OSM XML timestamp
    ('YYYY-MM-DDTHH:MM:SSZ')."""
    return '{:{dfmt}T{tfmt}Z}'.format(datetime.datetime.now(), dfmt='%Y-%m-%d', tfmt='%H:%M:%S')


def query_osm_city_name_gpd(session, lon, lat):
    """Look up the name of the OSM admin_level=8 (settlement) boundary polygon
    containing a point, from the local osm2pgsql-imported database.

    Note: the query text builds the point as ST_MakePoint(:lat, :lon), while the
    params bind :lon to the lon argument and :lat to the lat argument - the
    coordinates end up swapped compared to query_osm_postcode_gpd()'s (correct)
    ST_MakePoint(:lon, :lat). This looks like a bug; not changed here since this is a
    documentation-only pass.

    Args:
        session: SQLAlchemy session.
        lon: Point longitude.
        lat: Point latitude.

    Returns:
        str | None: The matching polygon's name, or None if lon/lat are missing or
        nothing matched.
    """
    if lat is None or lat == '' or lon == '' or lon is None:
        return None
    query = sqlalchemy.text('''
        SELECT name
        FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lat,:lon),4326) as geom) point
        WHERE admin_level='8' and ST_Contains(way, point.geom) ORDER BY name LIMIT 1;''')
    try:
        data = session.execute(query, {'lon': float(lon), 'lat': float(lat)}).first()
    except Exception as e:
        logging.error(e)
        logging.exception('Exception occurred')
    finally:
        session.commit()
    if data is None:
        return None
    else:
        return data[0]


def query_osm_city_name(session, name):
    """Check whether an OSM admin_level=8 (settlement) boundary polygon with this
    exact name exists in the local osm2pgsql-imported database - used by several
    providers to validate a harvested city name before trusting it.

    Args:
        session: SQLAlchemy session.
        name (str): City name to look up (exact match).

    Returns:
        str | None: The matching name (i.e. `name` echoed back) if found, else None.
    """
    query = sqlalchemy.text('''
        SELECT name
        FROM planet_osm_polygon WHERE admin_level='8' and name=:name ORDER BY name LIMIT 1;''')
    try:
        data = session.execute(query, {'name': name}).first()
    except Exception as e:
        logging.error(e)
        logging.exception('Exception occurred')
    finally:
        session.commit()
    if data is None:
        return None
    else:
        return data[0]
