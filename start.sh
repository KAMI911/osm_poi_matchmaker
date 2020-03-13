#!/bin/bash

mkdir -p ./container/{dbdata,importdata,osmdata,downloaddata}

docker pull postgis/postgis:latest

docker-compose up 2>&1 | tee -a ./import.log
