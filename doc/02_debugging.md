# Debugging the tool

Since OSM POI Matchmaker was written in Python, you can use any of your favorite tools to debug it. 
I prefer to use the free version of PyCharm.

## In PyCharm

1. Open PyCharm
1. Open _Settings_ → _Plugins_ → _Marketplace_
1. Search for `23257-lsp4ij`
1. Click on _Install_
1. Close _Settings_
1. Open _Run_ → _Edit Configurations…_
1. Click on _Add New Configuration_ (`+` icon)
1. Click on _Debug Adapter Protocol_
1. Enter `Docker` as _Name_
1. Click on _Server_ → _Use an Existing Debug Adapter Server_ → _NONE_
1. Select _Python - Debugpy_ from the dropdown list
1. Click on _Configuration_
1. Select _Attach_ as the _Debug Mode_
1. Paste this in the _DAP Parameters (JSON)_ text box:
    ```json
    {
        "name": "Attach",
        "type": "python",
        "request": "attach",
        "redirectOutput": true,
        "connect": {
            "host": "127.0.0.1",
            "port": 5678
        },
        "pathMappings": [
            {
                "localRoot": "${workspaceFolder}",
                "remoteRoot": "/opm/"
            }
        ]
    }
    ```
1. Click on _OK_
1. Run `osm_poi_matchmaker`
1. Wait until you see logs from the `opm_poi_load_app` container
1. Click on _Run_ → _Debug 'Docker'_
1. If you see a message like `ptvsd` and/or `debugpy`, you are connected successfully!

## Current number of importeble POIs grouped by POI names

```
SELECT pc.pc_id, poi_name, COUNT(pa.pa_id)
  FROM poi_address as pa
  FULL OUTER JOIN poi_common as pc
    ON pa.poi_common_id = pc.pc_id
  GROUP BY pc.pc_id, poi_name
  ORDER BY poi_name, pc.pc_id;
```

## Current number of importable POIs grouped by POI names from POI RAW table

```
SELECT pc.pc_id, pc.poi_common_name, COUNT(pa.pa_id)
  FROM poi_address_raw as pa
  FULL OUTER JOIN poi_common as pc
    ON pa.poi_common_id = pc.pc_id
  GROUP BY pc.pc_id, pc.poi_common_name
  ORDER BY pc.poi_common_name, pc.pc_id;
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

## Additional indexes

```
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS i_street_type ON street_type(street_type);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_way ON planet_osm_point(way);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addr ON planet_osm_point("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_point_addrcon ON planet_osm_point("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_point_amenity ON planet_osm_point(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_shop ON planet_osm_point(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_name ON planet_osm_point(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_point_brand ON planet_osm_point(brand);

CREATE INDEX IF NOT EXISTS i_planet_osm_line_way ON planet_osm_line using gist(way);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addr ON planet_osm_line("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_line_addrcon ON planet_osm_line("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_line_amenity ON planet_osm_line(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_shop ON planet_osm_line(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_name ON planet_osm_line(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_line_brand ON planet_osm_line(brand);


CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_way ON planet_osm_polygon using gist (way);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addr ON planet_osm_polygon("addr:postcode","addr:city","addr:street","addr:housenumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_addrcon ON planet_osm_polygon("addr:city","addr:conscriptionnumber");
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_amenity ON planet_osm_polygon(amenity);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_shop ON planet_osm_polygon(shop);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_name ON planet_osm_polygon(name);
CREATE INDEX IF NOT EXISTS i_planet_osm_polygon_brand ON planet_osm_polygon(brand);
```
