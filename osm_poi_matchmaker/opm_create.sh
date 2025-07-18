#!/bin/sh

OUTPUT_DIR="/opm/osm/"
sleep 30
while [ -f "${OUTPUT_DIR}/osm_download.lock" -o -f "${OUTPUT_DIR}/osm_import.lock" ]; do
  echo "Waiting for OSM import to be finished."
  sleep 60
done
echo "Starting the matchmaking…"
python3 -m debugpy --listen 0.0.0.0:5678 -m cProfile -o /opm/osm/profile_results.prof -s time ./create_db.py
