#!/bin/bash

OUTPUT_DIR="../download"

mkdir -p ${OUTPUT_DIR}
rm ${OUTPUT_DIR}/*.osm.pbf
wget -P ${OUTPUT_DIR}/ https://download.geofabrik.de/europe/hungary-latest.osm.pbf
