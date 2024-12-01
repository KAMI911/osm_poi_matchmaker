
FROM alpine:3.11.3

LABEL maintainer Kálmán Szalai (KAMI) <kami911@gmail.com>

ENV PATH /usr/local/bin:$PATH
ENV LANG C.UTF-8

RUN rm -rf /var/cache/apk/* && \
    rm -rf /tmp/*

RUN apk --no-cache update \
    apk add apk-tools && \
    apk add bash g++ make cmake openssl expat-dev bzip2-dev zlib-dev boost-dev postgresql-dev lua-dev proj-dev wget boost

# install osm2pgsql
ENV OSM2PGSQL_VERSION 1.6.0

RUN wget --no-check-certificate https://github.com/openstreetmap/osm2pgsql/archive/${OSM2PGSQL_VERSION}.tar.gz && \
    tar xzvf ${OSM2PGSQL_VERSION}.tar.gz && \
    cd /osm2pgsql-${OSM2PGSQL_VERSION} && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make && make install

# purge packages that are no longer needed
RUN apk del expat-dev bzip2-dev zlib-dev boost-dev g++ make openssl

RUN rm -rf /var/cache/apk/* && \
    rm -rf /tmp/*
RUN mkdir -p /opm/tools/ /opm/osm2pgsql
COPY ./tools /opm/tools/
COPY ./osm2pgsql /opm/osm2pgsql/
RUN ls -la /opm/tools/

WORKDIR /opm/tools

ENTRYPOINT ["/opm/tools/docker_downloadimport_osm_files.sh"]
