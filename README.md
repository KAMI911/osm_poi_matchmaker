# osm_poi_matchmaker
OSM POI Matchmaker

Download daily OpenStreetMap snapshots from here:

      https://download.geofabrik.de/europe.html

Import the daily snapshot of OSM database file into PostgreSQL database (for example in Hungary):

      osm2pgsql -c -m -s -d poi --style osm2pgsql/default.style --extra-attributes --multi-geometry -C 8000 -U poi -W -H localhost ~/Downloads/hungary-latest.osm
