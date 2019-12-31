
FROM alpine:3.6

MAINTAINER David Stefan <stefda@gmail.com>

ENV PATH /usr/local/bin:$PATH
ENV LANG C.UTF-8

RUN rm -rf /var/cache/apk/* && \
    rm -rf /tmp/*

RUN echo "@edge http://dl-cdn.alpinelinux.org/alpine/edge/testing/" >> /etc/apk/repositories

RUN apk --no-cache update && \
    apk add apk-tools@edge && \
    apk add bash g++ make cmake openssl

# add osm2pgsql dependencies
RUN apk add expat-dev bzip2-dev zlib-dev boost-dev postgresql-dev lua-dev proj-dev@edge wget@edge

# install osm2pgsql
ENV OSM2PGSQL_VERSION 1.2.1

RUN wget https://github.com/openstreetmap/osm2pgsql/archive/${OSM2PGSQL_VERSION}.tar.gz && \
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
