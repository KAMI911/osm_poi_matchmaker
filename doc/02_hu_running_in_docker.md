# Az osm_poi_matchmaker futtatása Docker-ben

Az osm_poi_matchmaker futtatása kényelmesen elvégezhető Docker-ben, ekkor a [Docker Compose segítségével kerül elindításra a működéshez szükséges három konténer](https://github.com/KAMI911/osm_poi_matchmaker/blob/master/docker-compose.yml).

## Alkalmazott konténerek

1. db konténer: az adatbázis konténer a PostGIS kiegészítővel PostgreSQL-t futtat
2. osm_load_app konténer: az OpenStreetMap adatokat betölti a PostgreSQL adatbázis
3. poi_load_app konténer: a külső adatforrások letöltését, betöltését és feldolgozását végzi.

A db nevű adatbázis konténernek végig futnia kell. Az osm_load_app nevű ([https://github.com/KAMI911/osm_poi_matchmaker/blob/master/tools/docker_downloadimport_osm_files.sh](letöltőscriptje)) konténer naponta egyszer [letölti az OpenStreetMap napi Magyarország kivágatát az internetről](https://download.geofabrik.de/europe/hungary-latest.osm.pbf), megvárja az adatbázis konténer elérhetőségét. Ez a konténer létrehoz egy lock fájlt, amelyet a poi_load_app konténer indítástól kezdve figyel, és csak akkor indul el a tényleges adat import, ha az OpenStreetMap adatbázis importja már befejeződött.

## A program beállításai

A program futtatásával kapcsolatos beállítások a [app.conf-template](https://github.com/KAMI911/osm_poi_matchmaker/blob/master/osm_poi_matchmaker/app.conf-template) fájlban találhatóak, amelyet átmásolva app.conf néven és a kívánt beállításokat szerkesztve elvégezhető a program testreszabása.

## A feldolgozás indítása

A feldolgozás elindítható a restart.sh nevű Bash script-tel.
