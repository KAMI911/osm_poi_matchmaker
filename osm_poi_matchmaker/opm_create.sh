#!/bin/sh

OUTPUT_DIR="/opm/osm/"
sleep 30
while [ -f "${OUTPUT_DIR}/osm_download.lock" -o -f "${OUTPUT_DIR}/osm_import.lock" ]
do
  echo "Waiting for OSM import to be finished."
  sleep 60
done
echo "Start the matchmaking ..."
python3 ./create_db.py
