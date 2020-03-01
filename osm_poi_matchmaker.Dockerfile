FROM buildpack-deps:stretch

LABEL maintainer Kálmán Szalai (KAMI) <kami911@gmail.com>

# ensure local python is preferred over distribution python
ENV PATH /usr/local/bin:$PATH

# http://bugs.python.org/issue19846
# > At the moment, setting "LANG=C" on a Linux system *fundamentally breaks Python 3*, and that's not OK.
ENV LANG C.UTF-8
# extra dependencies (over what buildpack-deps already includes)
RUN apt-get update && apt-get install -y --no-install-recommends \
  python3 \
  python3-pip \
  build-essential \
  python3-dev \
  python3-setuptools \
  python3-wheel \
  apt-utils \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /opm/requirements.txt

RUN pip3 install -r /opm/requirements.txt

COPY ./osm_poi_matchmaker /opm/osm_poi_matchmaker
ENV PYTHONPATH /opm/

HEALTHCHECK --interval=5m --timeout=3s \
  CMD nc opm_db 5432 || exit 1

CMD ["python3"]
WORKDIR /opm/osm_poi_matchmaker
ENTRYPOINT ["./opm_create.sh"]
