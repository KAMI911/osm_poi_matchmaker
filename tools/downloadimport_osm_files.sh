#!/bin/bash

OUTPUT_DIR="../download"

mkdir -p ${OUTPUT_DIR}
rm ${OUTPUT_DIR}/*.osm.pbf
wget -P ${OUTPUT_DIR}/ https://download.geofabrik.de/europe/hungary-latest.osm.pbf
osm2pgsql -c -m -s -d poi --style ../osm2pgsql/default.style --extra-attributes --multi-geometry -C 8000 -U poi -W -H localhost ${OUTPUT_DIR}/hungary-latest.osm.pbf
