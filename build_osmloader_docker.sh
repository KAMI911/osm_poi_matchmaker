#!/bin/bash

if [[ $(uname -m) == 'arm64' ]]; then
  export DOCKER_DEFAULT_PLATFORM=linux/amd64
fi

docker build -t osm_loader -f osm_loader.Dockerfile .
