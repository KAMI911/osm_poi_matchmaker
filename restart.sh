#!/bin/bash

# handling 'scram authentication requires libpq version 10 or above' bug
# upstream in libpg that's building against the wrong library version on ARM
if [[ $(uname -m) == 'arm64' ]]; then
  export DOCKER_DEFAULT_PLATFORM=linux/amd64
fi

docker stop opm_osm_load_app
docker stop opm_poi_load_app

./build_opm_docker.sh
./build_osmloader_docker.sh

echo "" >./import.log
docker-compose up 2>&1 | tee -a ./import.log
# docker stack deploy -c ./docker-compose.yml opm 2>&1 | tee -a ./import.log
