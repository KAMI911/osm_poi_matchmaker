# -*- coding: utf-8 -*-
"""
AllThePlaces.xyz Hungarian POI Importer

Data source:
  Entry point:  https://data.alltheplaces.xyz/runs/latest.json
  Insights:     {run.insights_url}  —  maps each brand's country splits to spider
                names and feature counts; used to identify HU-relevant spiders
  Spider data:  {base_url}/output/{spider_name}.geojson  (GeoJSON FeatureCollection)

Hungary filtering logic:
  1. A spider is considered HU-relevant when the insights file contains it under
     the "HU" key of the brand's atp_splits object.
  2. Each downloaded feature is accepted when its addr:country property equals
     "HU" (case-insensitive).  When addr:country is absent the feature is accepted
     if its coordinates fall inside the Hungarian bounding box
     (lat 45.7–48.6 °N, lon 16.1–22.9 °E).

Data format:
  Standard GeoJSON FeatureCollection per spider; coordinates follow the GeoJSON
  convention [longitude, latitude].  Address is typically in the combined field
  addr:street_address; opening_hours is already formatted for OpenStreetMap.

Edge cases:
  - Network or parse errors for individual spiders are logged and skipped;
    they do not abort the overall import.
  - addr:street_address is split into street + housenumber via the project's
    extract_street_housenumber_better_2 helper.
  - The insights_url is embedded in latest.json; a missing or malformed
    insights file causes the importer to log an error and exit cleanly.
  - Duplicate-detection relies on the deduplication built into insert_poi_dataframe
    (hash of poi_code + postcode + city + street + housenumber).
"""

try:
    import logging
    import sys
    import os
    import json

    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import (
        extract_street_housenumber_better_2,
        clean_city,
        clean_string,
    )
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)

# ── Constants ──────────────────────────────────────────────────────────────────

# Programmatic entry-point: returns run metadata including insights_url
ATP_LATEST_URL = 'https://data.alltheplaces.xyz/runs/latest.json'

# The builds page is the human-readable index of the same information;
# we document it here for reference but use the JSON API for automation.
ATP_BUILDS_PAGE = 'https://alltheplaces.xyz/builds.html'

# Bounding box for Hungary (approximate, used as fallback when addr:country is absent)
_HU_LAT_MIN, _HU_LAT_MAX = 45.7, 48.6
_HU_LON_MIN, _HU_LON_MAX = 16.1, 22.9

# Accepted country identifiers in AllThePlaces data
_HU_COUNTRY_CODES = frozenset({'HU', 'Hungary', 'hu'})

# Maximum poi_code length is constrained by the poi_common.poi_code column (Unicode 64);
# prefix "huatp_" (6 chars) leaves 58 characters for the spider name.
_ATP_CODE_PREFIX = 'huatp_'
_ATP_SPIDER_MAX_LEN = 57  # total poi_code ≤ 63 chars


# ── Module-level helpers ───────────────────────────────────────────────────────

def _spider_to_poi_code(spider_name: str) -> str:
    """Return a stable, DB-safe poi_code for the given AllThePlaces spider."""
    sanitised = spider_name.strip().lower().replace('-', '_').replace(' ', '_')
    return '{}{}'.format(_ATP_CODE_PREFIX, sanitised[:_ATP_SPIDER_MAX_LEN])


def _infer_poi_type(spider_name: str) -> str:
    """
    Heuristically map a spider name to the nearest internal poi_type value.

    AllThePlaces spider names typically encode the country suffix (e.g. *_hu)
    and sometimes the business category.  When no category keyword matches,
    the importer falls back to 'shop', which is the most common type for
    retail/commercial POIs.
    """
    n = spider_name.lower()
    if any(k in n for k in ('fuel', 'petrol', 'benzin', '_gas', 'mol_hu', 'shell', 'eni_', 'bp_')):
        return 'fuel'
    if any(k in n for k in ('atm',)):
        return 'atm'
    if any(k in n for k in ('bank',)):
        return 'bank'
    if any(k in n for k in ('pharmacy', 'patika', 'apotheke', 'benu', 'pingvin')):
        return 'pharmacy'
    if any(k in n for k in ('charging', '_ev_', 'electr')):
        return 'charging_station'
    if any(k in n for k in ('post_office', '_posta', 'postamt')):
        return 'post_office'
    if any(k in n for k in ('fastfood', 'fast_food', 'mcdonald', 'burger', 'kfc', 'subway_')):
        return 'fastfood'
    if any(k in n for k in ('chemist', 'droger', 'rossmann', 'dm_')):
        return 'chemist'
    if any(k in n for k in ('clothes', 'fashion', 'takko', 'pepco', 'jysk')):
        return 'clothes'
    if any(k in n for k in ('shoes', 'deichmann', 'ccc_')):
        return 'shoes'
    if any(k in n for k in ('optician', 'optic')):
        return 'optician'
    if any(k in n for k in ('diy', 'obi_', 'baumax', 'bauhaus')):
        return 'doityourself'
    if any(k in n for k in ('cosmetic', 'perfum', 'douglas', 'yves_rocher')):
        return 'cosmetics'
    if any(k in n for k in ('furniture', 'ikea',)):
        return 'furniture'
    if any(k in n for k in ('tobacco', 'dohany')):
        return 'tobacco'
    return 'shop'


def _derive_common_name(spider_name: str, brand_name: str | None) -> str:
    """Return a human-readable common name for this poi type."""
    if brand_name:
        return brand_name
    # Strip trailing country suffix (_hu, _hu_sk, …) and title-case
    parts = spider_name.split('_')
    # Drop trailing 2-letter (country) suffixes
    while parts and len(parts[-1]) == 2:
        parts.pop()
    return ' '.join(p.capitalize() for p in parts) if parts else spider_name.title()


def _is_hu_feature(props: dict, coords: list) -> bool:
    """
    Return True when the feature is located in Hungary.

    Primary check: addr:country property.
    Fallback:      geographic bounding box when addr:country is absent.
    """
    country = (
        props.get('addr:country')
        or props.get('@country')
        or props.get('country')
        or ''
    )
    if country:
        return country in _HU_COUNTRY_CODES
    # Fallback: bounding box (GeoJSON coords are [lon, lat])
    if len(coords) >= 2:
        lon, lat = float(coords[0]), float(coords[1])
        return _HU_LAT_MIN <= lat <= _HU_LAT_MAX and _HU_LON_MIN <= lon <= _HU_LON_MAX
    return False


# ── Main importer class ────────────────────────────────────────────────────────

class hu_alltheplaces(DataProvider):
    """
    Imports Hungarian Points of Interest from AllThePlaces (alltheplaces.xyz).

    Workflow
    --------
    1. ``contains()`` — sets the primary download URL (latest run JSON).
    2. ``types()``    — downloads run metadata and the insights file; discovers
                        all AllThePlaces spiders that have Hungarian POIs;
                        registers one poi_common type per spider.
    3. ``process()``  — downloads each spider's GeoJSON output, filters for
                        Hungary, and populates the internal POI dataset.
    4. ``export_list()`` (inherited) — bulk-inserts accumulated POIs.
    """

    # ── contains ──────────────────────────────────────────────────────────────

    def contains(self):
        # Primary download URL: AllThePlaces latest-run JSON manifest
        self.link = ATP_LATEST_URL
        self.tags = {'source': 'alltheplaces.xyz'}
        self.filetype = FileType.json
        self.filename = 'hu_alltheplaces_latest.json'

        # State populated during types(); consumed by process()
        self._hu_spiders: dict[str, str] = {}   # {spider_name: poi_common_name}
        self._base_output_url: str = ''          # e.g. .../runs/2026-02-28-13-32-34

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _download_latest_run(self) -> dict | None:
        """Download and return the latest run metadata JSON."""
        content = save_downloaded_soup(
            self.link,
            os.path.join(self.download_cache, self.filename),
            FileType.json,
        )
        if content is None:
            logging.error('[hu_alltheplaces] Cannot download latest run info from %s', self.link)
            return None
        try:
            return json.loads(content, strict=False)
        except (json.JSONDecodeError, ValueError) as e:
            logging.error('[hu_alltheplaces] Invalid JSON in latest run response: %s', e)
            return None

    def _download_insights(self, insights_url: str) -> dict | None:
        """Download and return the build insights JSON."""
        content = save_downloaded_soup(
            insights_url,
            os.path.join(self.download_cache, 'hu_alltheplaces_insights.json'),
            FileType.json,
        )
        if content is None:
            logging.error('[hu_alltheplaces] Cannot download insights from %s', insights_url)
            return None
        try:
            return json.loads(content, strict=False)
        except (json.JSONDecodeError, ValueError) as e:
            logging.error('[hu_alltheplaces] Invalid JSON in insights file: %s', e)
            return None

    def _extract_hu_spiders(self, insights: dict) -> dict[str, str]:
        """
        Parse the insights JSON and return a dict of
        {spider_name: brand_name} for all spiders that have Hungarian POIs.

        The insights file structure is::

            {"data": [{"nsi_brand": "Lidl", "atp_splits": {"HU": {"lidl_hu": 214}}}]}

        Multiple brand entries can reference the same spider; the first
        brand name encountered wins.
        """
        hu_spiders: dict[str, str] = {}
        data = insights.get('data', [])
        if not isinstance(data, list):
            logging.error('[hu_alltheplaces] Unexpected insights format: "data" is not a list.')
            return hu_spiders

        for brand in data:
            brand_name = brand.get('nsi_brand') or brand.get('q_title') or ''
            atp_splits = brand.get('atp_splits') or {}
            hu_entries = atp_splits.get('HU') or {}
            if not isinstance(hu_entries, dict):
                continue
            for spider_name in hu_entries:
                if spider_name and spider_name not in hu_spiders:
                    hu_spiders[spider_name] = brand_name

        logging.info(
            '[hu_alltheplaces] Found %d HU-relevant spiders in insights file.',
            len(hu_spiders),
        )
        return hu_spiders

    # ── types ─────────────────────────────────────────────────────────────────

    def types(self) -> list:
        """
        Resolve the latest AllThePlaces build, identify Hungary-relevant spiders
        from the insights file, and return one poi_common type definition per spider.
        """
        run_meta = self._download_latest_run()
        if run_meta is None:
            logging.warning('[hu_alltheplaces] No run metadata; returning empty type list.')
            self.__types = []
            return self.__types

        run_id = run_meta.get('run_id', '')
        insights_url = run_meta.get('insights_url', '')
        output_url = run_meta.get('output_url', '')

        if not run_id or not insights_url:
            logging.error(
                '[hu_alltheplaces] Missing run_id or insights_url in latest.json: %s',
                run_meta,
            )
            self.__types = []
            return self.__types

        # Derive the per-spider base URL from the output ZIP URL
        # e.g. .../runs/2026-02-28-13-32-34/output.zip → .../runs/2026-02-28-13-32-34
        if output_url:
            self._base_output_url = output_url.rsplit('/output', 1)[0]
        else:
            # Fallback: construct from insights_url base
            self._base_output_url = insights_url.rsplit('/stats', 1)[0]

        insights = self._download_insights(insights_url)
        if insights is None:
            self.__types = []
            return self.__types

        self._hu_spiders = self._extract_hu_spiders(insights)

        self.__types = []
        seen_codes: set[str] = set()

        for spider_name, brand_name in self._hu_spiders.items():
            poi_code = _spider_to_poi_code(spider_name)

            # Guard against accidental poi_code collisions after truncation
            if poi_code in seen_codes:
                logging.warning(
                    '[hu_alltheplaces] poi_code collision for spider %s (code %s); skipping.',
                    spider_name, poi_code,
                )
                continue
            seen_codes.add(poi_code)

            poi_type = _infer_poi_type(spider_name)
            common_name = _derive_common_name(spider_name, brand_name)

            # Derive a search name: brand name (lower-case) or spider name
            # without the trailing country suffix
            if brand_name:
                search_name = brand_name.lower()
            else:
                parts = spider_name.lower().split('_')
                while parts and len(parts[-1]) == 2:
                    parts.pop()
                search_name = ' '.join(parts)

            poi_tags = {
                'source': 'alltheplaces.xyz',
                'source:spider': spider_name,
            }
            if brand_name:
                poi_tags['brand'] = brand_name

            self.__types.append({
                'poi_code': poi_code,
                'poi_common_name': common_name,
                'poi_type': poi_type,
                'poi_tags': poi_tags,
                'poi_url_base': 'https://alltheplaces.xyz',
                'poi_search_name': search_name,
                'osm_search_distance_perfect': 200,
                'osm_search_distance_safe': 100,
                'osm_search_distance_unsafe': 20,
            })

        logging.info(
            '[hu_alltheplaces] Registered %d poi types for Hungarian spiders.',
            len(self.__types),
        )
        return self.__types

    # ── process ───────────────────────────────────────────────────────────────

    def process(self):
        """Download and import POIs from every HU-relevant AllThePlaces spider."""
        if not self._hu_spiders:
            logging.warning('[hu_alltheplaces] No HU-relevant spiders; nothing to process.')
            return

        if not self._base_output_url:
            logging.error(
                '[hu_alltheplaces] base_output_url not set — was types() called first?'
            )
            return

        for spider_name, _brand_name in self._hu_spiders.items():
            try:
                self._process_spider(spider_name)
            except Exception as e:
                logging.error(
                    '[hu_alltheplaces] Unhandled error processing spider %s: %s',
                    spider_name, e,
                )
                logging.exception('Exception occurred')

    def _process_spider(self, spider_name: str):
        """Download a single spider's GeoJSON and import Hungarian features."""
        spider_url = '{}/output/{}.geojson'.format(
            self._base_output_url, spider_name
        )
        # Cache file name: prefix to avoid collisions with other importers
        spider_cache_file = os.path.join(
            self.download_cache,
            'hu_alltheplaces_{}.json'.format(spider_name),
        )
        poi_code = _spider_to_poi_code(spider_name)

        logging.info('[hu_alltheplaces] Downloading spider: %s', spider_name)
        content = save_downloaded_soup(spider_url, spider_cache_file, FileType.json)
        if content is None:
            logging.warning(
                '[hu_alltheplaces] No data for spider %s; skipping.', spider_name
            )
            return

        features = self._parse_geojson(content, spider_name)
        if features is None:
            return

        imported = 0
        for feature in features:
            try:
                if self._import_feature(feature, poi_code):
                    imported += 1
            except Exception as e:
                logging.error(
                    '[hu_alltheplaces] Error importing feature from %s: %s',
                    spider_name, e,
                )
                logging.exception('Exception occurred')

        logging.info(
            '[hu_alltheplaces] Imported %d Hungarian POIs from spider %s.',
            imported, spider_name,
        )

    # ── GeoJSON parser ─────────────────────────────────────────────────────────

    def _parse_geojson(self, content: str, spider_name: str) -> list | None:
        """
        Parse GeoJSON content and return a list of Feature dicts.

        Supports:
        - Standard GeoJSON FeatureCollection  {"type":"FeatureCollection","features":[…]}
        - Single GeoJSON Feature              {"type":"Feature",…}
        - NDJSON (newline-delimited features, one per line)
        """
        try:
            data = json.loads(content, strict=False)
            if isinstance(data, dict):
                if data.get('type') == 'FeatureCollection':
                    return data.get('features') or []
                if data.get('type') == 'Feature':
                    return [data]
            logging.warning(
                '[hu_alltheplaces] Unexpected root JSON type for spider %s; '
                'trying NDJSON fallback.',
                spider_name,
            )
        except (json.JSONDecodeError, ValueError):
            pass  # fall through to NDJSON

        # NDJSON fallback: one JSON object per line
        features = []
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line, strict=False)
                if isinstance(obj, dict) and obj.get('type') == 'Feature':
                    features.append(obj)
            except (json.JSONDecodeError, ValueError):
                pass

        if features:
            return features

        logging.error(
            '[hu_alltheplaces] Cannot parse content for spider %s.', spider_name
        )
        return None

    # ── Feature importer ───────────────────────────────────────────────────────

    def _import_feature(self, feature: dict, poi_code: str) -> bool:
        """
        Map a GeoJSON Feature to the internal POI model and stage it for insert.

        Returns True when the feature was accepted and staged.
        """
        props = feature.get('properties') or {}
        geometry = feature.get('geometry') or {}

        # Only Point geometries carry a usable location
        if geometry.get('type') != 'Point':
            return False

        coords = geometry.get('coordinates') or []
        if len(coords) < 2:
            return False

        # Hungary filter (primary: addr:country; fallback: bounding box)
        if not _is_hu_feature(props, coords):
            return False

        # GeoJSON coordinates are [longitude, latitude]; check_hu_boundary(lat, lon)
        raw_lat = float(coords[1])
        raw_lon = float(coords[0])
        lat, lon = check_hu_boundary(raw_lat, raw_lon)
        if lat is None or lon is None:
            logging.debug(
                '[hu_alltheplaces] Feature outside HU boundary: lat=%s lon=%s',
                raw_lat, raw_lon,
            )
            return False

        # ── Populate POI fields ────────────────────────────────────────────────

        self.data.code = poi_code
        self.data.lat = lat
        self.data.lon = lon

        # Name: prefer 'name' property; fall back to brand
        name_val = clean_string(props.get('name', '') or props.get('brand', '') or '')
        self.data.name = name_val

        # Branch: use the spider ID as the data-source reference
        self.data.branch = clean_string(props.get('@spider', ''))

        # Address — AllThePlaces typically provides addr:street_address (combined)
        # but individual addr:street / addr:housenumber are preferred when present.
        addr_street = clean_string(props.get('addr:street', ''))
        addr_housenumber = clean_string(props.get('addr:housenumber', ''))
        addr_combined = clean_string(
            props.get('addr:street_address', '')
            or props.get('addr:full', '')
        )

        if addr_street:
            # Separate fields available — use them directly
            self.data.street = addr_street
            self.data.housenumber = addr_housenumber
            self.data.original = '{} {}'.format(addr_street, addr_housenumber).strip()
        elif addr_combined:
            # Combined address — split using the project's address parser
            self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                extract_street_housenumber_better_2(addr_combined)
            self.data.original = addr_combined
        # No address information: leave street/housenumber as None (cleared by add())

        self.data.city = clean_city(clean_string(props.get('addr:city', '') or ''))
        self.data.postcode = clean_string(props.get('addr:postcode', '') or '')

        # Contact
        self.data.phone = clean_string(props.get('phone', '') or props.get('contact:phone', '') or '')
        self.data.email = clean_string(props.get('email', '') or props.get('contact:email', '') or '')
        self.data.website = clean_string(
            props.get('website', '')
            or props.get('contact:website', '')
            or props.get('@source_uri', '')
            or ''
        )

        # Unique reference from the source spider
        self.data.ref = clean_string(props.get('ref', '') or props.get('@id', '') or '')

        # Opening hours: AllThePlaces already outputs OSM-format opening_hours
        oh = clean_string(props.get('opening_hours', '') or '')
        if oh:
            self.data.opening_hours = oh

        # Stage the record; clear_all() is called automatically by add()
        self.data.add()
        return True
