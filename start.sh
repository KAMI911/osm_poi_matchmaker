#!/bin/bash

mkdir -p ./container/{dbdata,importdata,osmdata,downloaddata}

docker pull postgis/postgis:13-3.0

docker compose up 2>&1 | tee -a ./import.log
