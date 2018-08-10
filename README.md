# osm_poi_matchmaker

[![Build Status](https://api.travis-ci.org/KAMI911/osm_poi_matchmaker.svg?branch=master)](https://travis-ci.org/KAMI911/osm_poi_matchmaker/) [![Code Climate](https://codeclimate.com/github//KAMI911/osm_poi_matchmaker/KAMI911/osm_poi_matchmaker//KAMI911/osm_poi_matchmaker/badges/gpa.svg)](https://codeclimate.com/github//KAMI911/osm_poi_matchmaker)

## Table of Contents

1. [About][About]
2. [Licensing][Licensing]
3. [Installation][Installation]
4. [Documentation][Documentation]
5. [Support][Support]
6. [Contributing][Contributing]

## About

The osm_poi_matchmaker is an open source OpenStreetMap import tool that try to import data
from external sources to OpenStreetMap database. The command line util creates OSM files
that can be imported with JOSM application.

Since this is a mass data import tool, before any importation task, please refer and follow
the [OpenStreetMap Import Guidlines](https://wiki.openstreetmap.org/wiki/Import/Guidelines).

This project has created by OpenStreetMap Contributors. You can [follow current imports](https://wiki.openstreetmap.org/wiki/WikiProject_Hungary/Import%C3%A1l%C3%A1s/POI_adatok)
related to this project.

*NOTE*: The osm_poi_matchmaker using manually created importers (called data provider). Currenty
osm_poi_matchmaker has only Hungarian data sources. You can use these data providers as examle
to create your own data providers.

**BIG FAT WARNING**: You must obtain proper permissions and licenses to use the data in OSM from the
data owner. If the license of the data is not compatible with the OpenStreetMap [Open Database License](https://wiki.openstreetmap.org/wiki/Open_Database_License),
you can not use the data. Many localities already have progressive open data policies.
Others have data policies that are almost open, but have conflicts with issues like prohibitions
on commercial use or requirements for attribution. Sometimes, getting permission to use data, even
if the existing license might seem prohibitive, is as simple as asking the appropriate authority if
they are willing to comply with the terms of the OpenStreetMap [Open Database License](https://wiki.openstreetmap.org/wiki/Open_Database_License). See
[Import/GettingPermission](https://wiki.openstreetmap.org/wiki/Import/GettingPermission) for example emails that touch on important issues. See also [ODbL
Compatibility](https://wiki.openstreetmap.org/wiki/Import/ODbL_Compatibility) for a quick view of some compatible and incompatible licenses of data to import.

## Licensing

The osm_poi_matchmaker application and documantations are licensed under the terms of the GNU
General Public License Version 3, you will find a copy of this license in the
[LICENSE](LICENSE) file included in the source package.

## Installation

You can try osm_poi_matchmaker with following steps.

* Checkout the source code from <https://github.com/KAMI911/osm_poi_matchmaker>

      git clone https://github.com/KAMI911/osm_poi_matchmaker.git

* Install PostgreSQL 9.6 or newer and PostGIS.

* Download daily OpenStreetMap snapshots from here (We are using Hungarian extract):

      https://download.geofabrik.de/europe.html

* Import the daily snapshot of OSM database file into PostgreSQL database (for example in Hungary):

      osm2pgsql -c -m -s -d poi --style osm2pgsql/default.style --extra-attributes --multi-geometry -C 8000 -U poi -W -H localhost ~/Downloads/hungary-latest.osm

## Documentation

The documentation is located in the [doc/](doc/) directory.

## Support

If you have any question, do not hesitate and drop me a line.
If you found a bug, or have a feature request, you can [fill an issue](https://github.com/KAMI911/osm_poi_matchmaker/issues).

## Contributing

There are many ways to contribute to osm_poi_matchmaker -- whether it be sending patches,
testing, reporting bugs, or reviewing and updating the documentation. Every
contribution is appreciated!

Please continue reading in the [contributing chapter](CONTRIBUTING.md).

<!-- TOC URLs -->
[About]: #about
[Licensing]: #licensing
[Installation]: #installation
[Documentation]: #documentation
[Support]: #support
[Contributing]: #contributing

