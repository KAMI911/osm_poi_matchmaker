#!/bin/bash

docker run --name=opm_db -d -e POSTGRES_USER=poi -e POSTGRES_PASS=poitest -e POSTGRES_DBNAME=poi -e ALLOW_IP_RANGE=0.0.0.0/0 -p 15432:5432 -v opm_dbdata:/var/lib/postgresql/data --restart=always postgis/postgis:14-3.2
