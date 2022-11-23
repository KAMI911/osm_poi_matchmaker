#!/bin/bash

DOWNLOAD_URL="https://download.geofabrik.de/europe/"
DOWNLOAD=0
FILE="hungary-latest.osm.pbf"
DOWNLOAD_FILE="${DOWNLOAD_URL}/${FILE}"
OUTPUT_DIR="/opm/osm/"
DOWNLOAD_DS_FILE="${OUTPUT_DIR}/download_daystamp"
IMPORT_DS_FILE="${OUTPUT_DIR}/import_daystamp"
CURRENT_DAYSTAMP=$(date +%Y%m%d)

mkdir -p ${OUTPUT_DIR}

if [ -f "${DOWNLOAD_DS_FILE}" ]; then
  DAYSTAMP=$(cat ${DOWNLOAD_DS_FILE})
  if [ "${DAYSTAMP}" == "${CURRENT_DAYSTAMP}" ]; then
    DOWNLOAD=0
  else
    echo "${CURRENT_DAYSTAMP}" >"${DOWNLOAD_DS_FILE}"
    DOWNLOAD=1
  fi
else
  echo "${CURRENT_DAYSTAMP}" >"${DOWNLOAD_DS_FILE}"
  DOWNLOAD=1
fi
if [ ! -f "${OUTPUT_DIR}/${FILE}" ]; then
  DOWNLOAD=1
fi
if [ -f "${IMPORT_DS_FILE}" ]; then
  IMPORT_DAYSTAMP=$(cat ${IMPORT_DS_FILE})
  if [ "${IMPORT_DAYSTAMP}" == "${CURRENT_DAYSTAMP}" ]; then
    IMPORT=0
  else
    echo "${CURRENT_DAYSTAMP}" >"${IMPORT_DS_FILE}"
    IMPORT=1
  fi
else
  echo "${CURRENT_DAYSTAMP}" >"${IMPORT_DS_FILE}"
  IMPORT=1
fi
if [ "${DOWNLOAD}" -eq "1" ]; then
  echo "Will download OSM files"
fi
if [ -f "${OUTPUT_DIR}/do_osm_download" -a "${DOWNLOAD}" -eq "1" ]; then
  echo "Downloading OSM files"
  mkdir -p ${OUTPUT_DIR}
  touch "${OUTPUT_DIR}/osm_download.lock"
  wget -O ${OUTPUT_DIR}/${FILE} ${DOWNLOAD_FILE}
  echo "  wget -O ${OUTPUT_DIR}/${FILE}.md5  ${DOWNLOAD_FILE}.md5"
  wget -O ${OUTPUT_DIR}/${FILE}.md5 ${DOWNLOAD_FILE}.md5
  rm "${OUTPUT_DIR}/osm_download.lock"
fi
if [ -f "${OUTPUT_DIR}/do_osm_import" -a "${IMPORT}" -eq "1" ]; then
  touch "${OUTPUT_DIR}/osm_import.lock"
  cd ${OUTPUT_DIR}
  md5sum -c ${OUTPUT_DIR}/${FILE}.md5
  exitcode=${?}
  if [ "$exitcode" == "0" ]; then
    sleep 15
    osm2pgsql -v -c -l -s -d poi --style /opm/osm2pgsql/default.style --number-processes 12 --extra-attributes --multi-geometry --cache-strategy optimized -C 800 -U poi -H opm_db ${OUTPUT_DIR}/${FILE}
    exitcode=${?}
    if [ "$exitcode" != "0" ]; then
      echo "ERROR occurred during OSM import!"
      exit 10
    else
      rm "${OUTPUT_DIR}/osm_download.lock" "${OUTPUT_DIR}/osm_import.lock"
    fi
  else
    echo "Non zero exit code for md5sum, please re-download the OSM pbf file."
    exit 5
  fi
else
  echo "Skipping OSM import."
  rm "${OUTPUT_DIR}/osm_download.lock" "${OUTPUT_DIR}/osm_import.lock"
fi
exit 0
