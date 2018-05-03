# osm_poi_matchmaker
OSM POI Matchmaker

Import the daily snapshot of OSM database file into PostgreSQL database (for example in Hungary):

      osm2pgsql -c -m -s -d osm -C 8000 -U poi -W -H localhost ~/Downloads/hungary-latest.osm
