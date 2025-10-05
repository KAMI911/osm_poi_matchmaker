FROM python:3.11-slim-bullseye

LABEL maintainer="Kálmán Szalai (KAMI) <kami911@gmail.com>"

# ensure local python is preferred over distribution python
ENV PATH=/usr/local/bin:$PATH

ENV LANG=C
# extra dependencies (over what buildpack-deps already includes)
RUN apt-get update && apt-get install -y --no-install-recommends \
   build-essential \
   apt-utils  \
   libgdal-dev \
   libxml2-dev \
   libxslt-dev \
 && python -m pip install --upgrade pip \
 && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /opm/requirements.txt

RUN python3.11 --version && \
    python3.11 -m pip --version && \
    python3.11 -m pip install -r /opm/requirements.txt

COPY ./osm_poi_matchmaker /opm/osm_poi_matchmaker
ENV PYTHONPATH=/opm/

HEALTHCHECK --interval=5m --timeout=3s \
  CMD nc opm_db 5432 || exit 1

CMD ["python3"]
WORKDIR /opm/osm_poi_matchmaker
ENTRYPOINT ["./opm_create.sh"]
