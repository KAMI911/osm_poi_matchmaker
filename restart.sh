#!/bin/bash

docker stop opm_osm_load_app &
docker stop opm_poi_load_app &

./build_opm_docker.sh
./build_osmloader_docker.sh

echo "" >./import.log
docker-compose up 2>&1 | tee -a ./import.log
# docker stack deploy -c ./docker-compose.yml opm 2>&1 | tee -a ./import.log
