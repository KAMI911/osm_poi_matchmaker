# Debugging of the tool

Since OSM POI Matchmaker was written in Python languege, you can use any of your favorite tool to debug it. I am using PyCharm Community edition. This is a free tool for coding yout Python project.

## Current number of importeble POIs grouped by POI names

```
SELECT pc.pc_id, poi_name, COUNT(pa.pa_id)
  FROM poi_address as pa
  FULL OUTER JOIN poi_common as pc
    ON pa.poi_common_id = pc.pc_id
  GROUP BY pc.pc_id, poi_name
  ORDER BY poi_name, pc.pc_id;
```

# Debugging SQL queries

There are a few longer but not so complex SQL squeries that are essential parts of the POI matching mechanism. Fine tuning of those queries are key to create useful output files.

Few examples that can you test instantly, or changing parameters to follow your requirements.

## Examples

### Original query #1 in the code

```
SELECT name, osm_id, {metadata_fields} 'way' AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat),4326) as geom) point
WHERE ({query_type}) AND osm_id > 0 {query_name}
    AND ST_DWithin(ST_Buffer(way,:buffer),ST_Transform(point.geom,3857), :distance)
```

#### First example of query #1 parameters

7615  id="-462"   http://www.openstreetmap.org/node/3387882908
46.066829 18.226012

```
SELECT name, osm_id, 'way' AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(18.226012, 46.066829),4326) as geom) point
WHERE (amenity='post_office') AND osm_id > 0  AND name ~* 'posta'
    AND ST_DWithin(ST_Buffer(way,10),ST_Transform(point.geom,3857), 400)
```

#### Second example of query #1 parameters

7602  id="-590"   http://www.openstreetmap.org/way/37103651
46.064675 18.187093

```
SELECT name, osm_id, 'node' AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt
FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(18.187093, 46.064675),4326) as geom) point
WHERE (amenity='post_office') AND osm_id > 0  AND name ~* 'posta'
    AND ST_DWithin(ST_Buffer(way,10),ST_Transform(point.geom,3857), 360)
```

#### Third example of query #1 parameters

7607  id="-456"   http://www.openstreetmap.org/node/1700241654
46.085841 18.261615

```
SELECT name, osm_id, 'node' AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt
FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(18.261615, 46.085841),4326) as geom) point
WHERE (amenity='post_office') AND osm_id > 0  AND name ~* 'posta'
    AND ST_DWithin(ST_Buffer(way,10),ST_Transform(point.geom,3857), 350)
```

#### Fourth example of query #1 parameters

Csabacsűd post office

```
SELECT name, osm_id, 'node' AS node, shop, amenity, "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street", ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,  ST_AsEWKT(way) as way_ewkt
FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint( 20.645925, 46.82541),4326) as geom) point
WHERE (amenity='post_office') AND osm_id > 0  AND name ~* 'posta'
    AND ST_DWithin(ST_Buffer(way,10),ST_Transform(point.geom,3857), 430)
```

### Original query #2 in the code


```
--- The way selector with street name
SELECT name, osm_id, {metadata_fields} 988 AS priority, 'way' AS node, shop, amenity, "addr:housename",
       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
WHERE ({query_type}) AND osm_id > 0 {query_name} {street_query}
    AND ST_DWithin(ST_Buffer(way,:buffer),ST_Transform(point.geom, 3857), :street_distance)
UNION
--- The node selector with street name
SELECT name, osm_id, {metadata_fields} 989 AS priority, 'node' AS node, shop, amenity, "addr:housename",
       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon,
       ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
WHERE ({query_type}) AND osm_id > 0 {query_name} {street_query}
    AND ST_DWithin(way,ST_Transform(point.geom, 3857), :street_distance)
UNION
--- The relation selector with street name
SELECT name, osm_id, {metadata_fields} 987 AS priority, 'relation' AS node, shop, amenity,
       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
WHERE ({query_type}) AND osm_id < 0 {query_name} {street_query}
    AND ST_DWithin(ST_Buffer(way,:buffer),ST_Transform(point.geom, 3857), :street_distance)
UNION
--- The way selector without street name
SELECT name, osm_id, {metadata_fields} 998 AS priority, 'way' AS node, shop, amenity, "addr:housename",
       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
WHERE ({query_type}) AND osm_id > 0 {query_name}
    AND ST_DWithin(ST_Buffer(way,:buffer),ST_Transform(point.geom, 3857), :distance)
UNION
--- The node selector without street name
SELECT name, osm_id, {metadata_fields} 999 AS priority, 'node' AS node, shop, amenity, "addr:housename",
       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon,
       ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
WHERE ({query_type}) AND osm_id > 0 {query_name}
    AND ST_DWithin(way,ST_Transform(point.geom, 3857), :distance)
UNION
--- The relation selector without street name
SELECT name, osm_id, {metadata_fields} 997 AS priority, 'relation' AS node, shop, amenity,
       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(:lon,:lat), 4326) as geom) point
WHERE ({query_type}) AND osm_id < 0 {query_name}
    AND ST_DWithin(ST_Buffer(way,:buffer),ST_Transform(point.geom, 3857), :distance)
ORDER BY priority ASC, distance ASC;
```

#### First example of query #2 parameters
```
--- The way selector with street name
SELECT name, osm_id, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp,  988 AS priority, 'way' AS node, shop, amenity, "addr:housename",
       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(21.623189,47.553246), 4326) as geom) point
WHERE (amenity='post_office') AND osm_id > 0  AND name ~* 'posta' AND "addr:street" = 'Nagyerdei körút'
    AND ST_DWithin(ST_Buffer(way,10),ST_Transform(point.geom, 3857), 900)
UNION
--- The node selector with street name
SELECT name, osm_id, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp,  989 AS priority, 'node' AS node, shop, amenity, "addr:housename",
       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon,
       ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(21.623189,47.553246), 4326) as geom) point
WHERE (amenity='post_office') AND osm_id > 0  AND name ~* 'posta' AND "addr:street" = 'Nagyerdei körút'
    AND ST_DWithin(way,ST_Transform(point.geom, 3857), 900)
UNION
--- The relation selector with street name
SELECT name, osm_id, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp,  987 AS priority, 'relation' AS node, shop, amenity,
       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(21.623189,47.553246), 4326) as geom) point
WHERE (amenity='post_office') AND osm_id < 0  AND name ~* 'posta' AND "addr:street" = 'Nagyerdei körút'
    AND ST_DWithin(ST_Buffer(way,10),ST_Transform(point.geom, 3857), 900)
UNION
--- The way selector without street name
SELECT name, osm_id, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp,  998 AS priority, 'way' AS node, shop, amenity, "addr:housename",
       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(21.623189,47.553246), 4326) as geom) point
WHERE (amenity='post_office') AND osm_id > 0  AND name ~* 'posta'
    AND ST_DWithin(ST_Buffer(way,10),ST_Transform(point.geom, 3857), 900)
UNION
--- The node selector without street name
SELECT name, osm_id, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp,  999 AS priority, 'node' AS node, shop, amenity, "addr:housename",
       "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, ST_X(ST_Transform(planet_osm_point.way,4326)) as lon,
       ST_Y(ST_Transform(planet_osm_point.way,4326)) as lat
FROM planet_osm_point, (SELECT ST_SetSRID(ST_MakePoint(21.623189,47.553246), 4326) as geom) point
WHERE (amenity='post_office') AND osm_id > 0  AND name ~* 'posta'
    AND ST_DWithin(way,ST_Transform(point.geom, 3857), 430)
UNION
--- The relation selector without street name
SELECT name, osm_id, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp,  997 AS priority, 'relation' AS node, shop, amenity,
       "addr:housename", "addr:housenumber", "addr:postcode", "addr:city", "addr:street",
       ST_Distance_Sphere(ST_Transform(way, 4326), point.geom) as distance, way,
       ST_AsEWKT(way) as way_ewkt, NULL as lon, NULL as lat
FROM planet_osm_polygon, (SELECT ST_SetSRID(ST_MakePoint(21.623189,47.553246), 4326) as geom) point
WHERE (amenity='post_office') AND osm_id < 0  AND name ~* 'posta'
    AND ST_DWithin(ST_Buffer(way,10),ST_Transform(point.geom, 3857), 430)
ORDER BY priority ASC, distance ASC;
```