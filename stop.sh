#!/bin/bash

docker compose down

docker rm opm_osm_load_app opm_poi_load_app opm_db
