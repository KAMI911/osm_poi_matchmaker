# osm_poi_matchmaker - OSM POI ⌘ 🎔 MatchMaker

[![Build Status](https://api.travis-ci.org/KAMI911/osm_poi_matchmaker.svg?branch=master)](https://travis-ci.org/KAMI911/osm_poi_matchmaker/)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/7fa248270de94705a4ddc956ca710ce3)](https://www.codacy.com/app/KAMI911/osm_poi_matchmaker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=KAMI911/osm_poi_matchmaker&amp;utm_campaign=Badge_Grade)
[![Maintainability](https://api.codeclimate.com/v1/badges/710ad0cde398623b864b/maintainability)](https://codeclimate.com/github/KAMI911/osm_poi_matchmaker/maintainability)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/KAMI911/osm_poi_matchmaker/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/KAMI911/osm_poi_matchmaker/?branch=master)
[![Build Status](https://scrutinizer-ci.com/g/KAMI911/osm_poi_matchmaker/badges/build.png?b=master)](https://scrutinizer-ci.com/g/KAMI911/osm_poi_matchmaker/build-status/master)
[![Percentage of issues still open](http://isitmaintained.com/badge/open/KAMI911/osm_poi_matchmaker.svg)](http://isitmaintained.com/project/KAMI911/osm_poi_matchmaker "Percentage of issues still open")
![GitHub stars](https://img.shields.io/github/stars/KAMI911/osm_poi_matchmaker?style=social)

## Table of Contents

1. [About][About]
2. [Licensing][Licensing]
3. [Installation][Installation]
4. [Environment variables][Environment variables]
5. [Documentation][Documentation]
6. [Support][Support]
7. [Contributing][Contributing]
8. [Donation][Donation]

## About

`osm_poi_matchmaker` is an open source OpenStreetMap import tool that tries to import data
from external sources to the OpenStreetMap database. The command line util creates OSM files
that can be imported with JOSM.

Since this is a mass data import tool, before any importation task, please refer to and follow
the [OpenStreetMap Import Guidelines](https://wiki.openstreetmap.org/wiki/Import/Guidelines).

This project was created by OpenStreetMap Contributors. You can [follow current imports](https://wiki.openstreetmap.org/wiki/Hungary/Importálás/POI_adatok)
related to this project.

*NOTE*: `osm_poi_matchmaker` uses manually created importers (called data providers). Currenty
`osm_poi_matchmaker` has only Hungarian data sources. You can use these data providers as an example
to create your own data providers.

**BIG FAT WARNING**: You must obtain proper permissions and licenses to use the data in OSM from the
data owner. If the license of the data is not compatible with the [Open Database License](https://wiki.openstreetmap.org/wiki/Open_Database_License),
you can not use the data. Many localities already have progressive open data policies.
Others have data policies that are almost open, but have conflicts with issues like prohibitions
on commercial use or requirements for attribution. Sometimes, getting permission to use data, even
if the existing license might seem prohibitive, is as simple as asking the appropriate authority if
they are willing to comply with the terms of the [Open Database License](https://wiki.openstreetmap.org/wiki/Open_Database_License). See
[Import/Getting permission](https://wiki.openstreetmap.org/wiki/Import/Getting_permission) for example emails that touch on important issues. See also [ODbL
Compatibility](https://wiki.openstreetmap.org/wiki/Import/ODbL_Compatibility) for a quick overview of some compatible and incompatible licenses of data to import.

## Licensing

The `osm_poi_matchmaker` application and its documentation is licensed under the terms of the GNU
General Public License Version 3. You will find a copy of this license in the
[LICENSE](LICENSE) file included in the source package.

## Installation

You can try `osm_poi_matchmaker` with the following steps:

* Checkout the source code from <https://github.com/KAMI911/osm_poi_matchmaker>:

      git clone https://github.com/KAMI911/osm_poi_matchmaker

* Install PostgreSQL 9.6 or newer and PostGIS.

* Download daily OpenStreetMap snapshots from here (we are using the Hungarian extract):

      https://download.geofabrik.de/europe.html

* Import the daily snapshot of the OSM database file into the PostgreSQL database. For example in Hungary:

      osm2pgsql -c -m -s -d poi --style osm2pgsql/default.style --extra-attributes --multi-geometry -C 8000 -U poi -W -H localhost ~/Downloads/hungary-latest.osm.pbf


## Environment variables

You can specify these environment variables to use instead of the configured parameters in the `app.conf` file.

	OPM_DIRECTORY_OUTPUT

Specify the output directory for processed and exported data (OSM and CSV files). This is the output directory.

	OPM_DIRECTORY_CACHE_URL

Specify the cache directory for downloaded external data (HTML, JS and CSV files).

	OPM_DATABASE_TYPE

Specify a supported database for storing matched data. Since OSM POI Matchmaker is using PostGIS,
only `postgresql` is supported and tested. The default value is `postgresql`.

	OPM_DATABASE_WRITE_HOST

Hostname or IP address of the database server. The default value is `localhost`.

	OPM_DATABASE_WRITE_PORT

The connection port of the database server. The default value is `5432`.

	OPM_DATABASE_WRITE_USERNAME

The username of the writing user for the database connection. The default value is `poi`.

	OPM_DATABASE_WRITE_PASSWORD

The password of the writing user for the database connection. The default value is `poitest`.

    OPM_DATABASE_POI_DATABASE

The name of the database to which the writing user will connect to. The default value is `poi`.

    OPM_DATAPROVIDERS_MODULES_AVAILABLE

Comma separated list of available data provider modules from the `dataprovider` folder.
Default values are:
hu_aldi,hu_avia,hu_benu,hu_budapest_bank,hu_cba,hu_cib_bank,hu_dm,hu_foxpost,hu_kh_bank,hu_kulcs_patika,hu_mobil_petrol,
hu_mol_bubi,hu_mol,hu_omv,hu_penny_market,hu_pepco,hu_posta_json,hu_posta,hu_rossmann,hu_shell,hu_spar,hu_tesco,hu_tom_market

    OPM_DATAPROVIDERS_MODULES_ENABLE

Comma separated list of available data provider modules from the `dataprovider` folder.
Only the enabled data providers will be imported, but all imported files will be exported.
Default values are:
hu_posta,hu_aldi,hu_avia,hu_benu,hu_budapest_bank,hu_cba,hu_cib_bank,hu_dm,hu_foxpost,hu_kh_bank,hu_kulcs_patika,hu_mobil_petrol,
hu_mol_bubi,hu_mol,hu_omv,hu_penny_market,hu_pepco,hu_posta_json,hu_rossmann,hu_shell,hu_spar,hu_tesco,hu_tom_market

## Importing with this tool

You can import the created OSM files in JOSM then upload them to the OpenStreetMap database.
Since this is a mass data import tool, before any importation task, please refer and follow
the [OpenStreetMap Import Guidelines](https://wiki.openstreetmap.org/wiki/Import/Guidelines).

Please use these import tags for changesets and edit them accordingly:

    comment=Import Hungarian POIs of something
    source=import;website
    import:tool:url=https://github.com/KAMI911/osm_poi_matchmaker
    import:tool:version=0.8.0
    import:tool=osm_poi_matchmaker
    import:area=hungary
    mechanical=yes
    is_in:country_code=HU
    is_in:country=Hungary
    import:discussion:url=https://lists.openstreetmap.org/pipermail/imports/2018-July/005591.html

## Documentation

The documentation is located in the [`doc/`](doc/) directory.

## Support

Should you have any questions, do not hesitate to drop me a line.
If you've found a bug, or have a feature request, you can [create an issue](https://github.com/KAMI911/osm_poi_matchmaker/issues).

## Contributing

There are many ways to contribute to `osm_poi_matchmaker`—sending patches,
testing, reporting bugs, or reviewing and updating the documentation. Every
contribution is appreciated!

### Fork me on GitHub

https://github.com/KAMI911/osm_poi_matchmaker

## Donation

If you find this useful, please consider a donation:

[![paypal](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=RLQZ58B26XSLA)

<!-- TOC URLs -->
[About]: #about
[Licensing]: #licensing
[Installation]: #installation
[Environment variables]: #Environment_variables
[Documentation]: #documentation
[Support]: #support
[Contributing]: #contributing
[Donation]: #donation
