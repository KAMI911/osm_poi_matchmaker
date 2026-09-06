# -*- coding: utf-8 -*-
"""
AllThePlaces.xyz Hungarian POI Importer

Data source:
  Entry point:    https://data.alltheplaces.xyz/runs/latest.json
  Insights:       {run.insights_url} - maps each brand's country splits to spider
                  names and feature counts; used to identify HU-relevant spiders
  Spider output:  https://data.alltheplaces.xyz/runs/latest/output/{spider}.geojson
                  - a documented, dedicated endpoint (see alltheplaces/API.md) that
                  302-redirects to that spider's *last successful* run, rather than
                  assuming every spider produced output in the single latest run
                  (many don't - a naive {run}/output/{spider}.geojson URL 404s for
                  those).

Hungary filtering logic:
  1. A spider is considered HU-relevant when the insights file contains it under
     the "HU" key of the brand's atp_splits object, UNLESS that brand is already
     covered by a dedicated hu_*.py provider elsewhere in this project (see
     EXCLUDED_BRANDS) - this importer exists to catch everything those don't,
     not to compete with them for the same poi_code.
  2. Each downloaded feature is accepted when its addr:country property equals
     "HU" (case-insensitive). When addr:country is absent the feature is accepted
     if its coordinates fall inside the Hungarian bounding box.

poi_type / poi_code:
  Unlike a hand-built provider, one spider's features can carry more than one real
  OSM category (e.g. a bank spider mixing amenity=bank and amenity=atm rows) - so
  poi_type is resolved per feature from its own amenity/shop/... tag (not guessed
  from the spider's name), and one poi_code is registered per (spider, poi_type)
  pair actually found in the data, not per spider.

  POI_common.poi_code is a hard Unicode(10) database column - nowhere near enough
  room for a readable "huatp_<spider>_<type>" code once spider names and multiple
  types are involved, so poi_code here is a deterministic hash ('atp' + 7 hex
  chars of sha1(spider:poi_type)) instead. It isn't human-readable, but it's
  stable across runs (same spider+type always hashes the same way, so re-imports
  keep matching the same poi_common row) and collisions are effectively
  impossible at this scale. poi_tags carries source:spider and source:osm_tag so
  the original spider/tag is still recoverable from the row itself.

Data format:
  Standard GeoJSON FeatureCollection per spider; coordinates follow the GeoJSON
  convention [longitude, latitude]. Address is typically in the combined field
  addr:street_address; opening_hours is already formatted for OpenStreetMap.

Edge cases:
  - Network or parse errors for individual spiders are logged and skipped; they
    do not abort the overall import.
  - addr:street_address is split into street + housenumber via the project's
    extract_street_housenumber_better_2 helper.
  - A feature whose only tag key isn't in _OSM_TAG_TO_POI_TYPE (and isn't a
    generic shop=* value) is skipped rather than forced into an unrelated type.
"""

try:
    import logging
    import sys
    import os
    import json
    import hashlib
    import re

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

ATP_LATEST_URL = 'https://data.alltheplaces.xyz/runs/latest.json'
ATP_SPIDER_OUTPUT_URL = 'https://data.alltheplaces.xyz/runs/latest/output/{}.geojson'

_HU_LAT_MIN, _HU_LAT_MAX = 45.7, 48.6
_HU_LON_MIN, _HU_LON_MAX = 16.1, 22.9
_HU_COUNTRY_CODES = frozenset({'HU', 'Hungary', 'hu'})

# Brands already covered by a dedicated hu_*.py provider elsewhere in this project -
# checked against ATP's nsi_brand/spider-derived name (normalised: lower-cased,
# diacritics folded, non-alphanumeric stripped) so this importer doesn't create a
# second, less-tuned, competing poi_code for the same real-world brand.
EXCLUDED_BRANDS = frozenset({
    'aldi', 'alzabox', 'amplifon', 'auchan', 'avia', 'benu', 'benugyogyszertar',
    'budapestbank', 'burgerking', 'cba', 'ccc', 'cibbank', 'decathlon', 'deichmann',
    'dm', 'dpd', 'easybox', 'erstebank', 'foxpost', 'foxpostabox', 'foxpostzbox',
    'freshcorner', 'gls', 'hm', 'handm', 'ikea', 'ionity', 'jysk', 'kfc', 'kh',
    'khbank', 'kulcspatikak', 'kulcspatika', 'lidl', 'magnetbank', 'magyarposta',
    'mavstart', 'mav', 'mbhbank', 'mkbbank', 'mobiliti', 'mobilpetrol',
    'mobilposta', 'mol', 'molbubi', 'mpl', 'newyorker', 'obi', 'ofotert', 'oil',
    'omv', 'omvemotion', 'otpbank', 'otp', 'packeta', 'packetazbox', 'penny',
    'pennymarket', 'pepco', 'pingvinpatika', 'pizzahut', 'posta', 'postapont',
    'prima', 'raiffeisenbank', 'raiffeisen', 'real', 'rossmann', 'sberbank',
    'shell', 'sinsay', 'spar', 'starbucks', 'takarekbank', 'takko', 'takkofashion',
    'tesco', 'tommarket', 'unicredit', 'volanbusz', 'yettel', 'yvesrocher', 'zara',
    'zbox',
})

_DIACRITIC_MAP = str.maketrans('áéíóöőúüű', 'aeiooouuu')

# Priority-ordered OSM tag keys checked per feature; the first one present decides
# the feature's type. (key, value) -> internal poi_type; a 'shop' key whose value
# isn't listed here still falls back to the generic 'shop' type (see
# _resolve_feature_type), everything else is skipped rather than mistagged.
_OSM_TAG_TO_POI_TYPE = {
    ('amenity', 'bank'): 'bank',
    ('amenity', 'atm'): 'atm',
    ('amenity', 'pharmacy'): 'pharmacy',
    ('amenity', 'fuel'): 'fuel',
    ('amenity', 'post_office'): 'post_office',
    ('amenity', 'fast_food'): 'fastfood',
    ('amenity', 'cafe'): 'cafe',
    ('amenity', 'restaurant'): 'restaurant',
    ('amenity', 'charging_station'): 'charging_station',
    ('amenity', 'bicycle_rental'): 'bicycle_rental',
    ('amenity', 'vending_machine'): 'vending_machine',
    ('amenity', 'fire_station'): 'fire_station',
    ('shop', 'chemist'): 'chemist',
    ('shop', 'clothes'): 'clothes',
    ('shop', 'fashion'): 'clothes',
    ('shop', 'shoes'): 'shoes',
    ('shop', 'doityourself'): 'doityourself',
    ('shop', 'hardware'): 'doityourself',
    ('shop', 'cosmetics'): 'cosmetics',
    ('shop', 'beauty'): 'cosmetics',
    ('shop', 'furniture'): 'furniture',
    ('shop', 'optician'): 'optician',
    ('shop', 'tobacco'): 'tobacco',
    ('shop', 'mobile_phone'): 'mobile_phone',
    ('shop', 'sports'): 'sports',
    ('shop', 'hearing_aids'): 'hearing_aids',
}
_TYPE_KEY_PRIORITY = ('amenity', 'shop', 'office', 'craft', 'tourism', 'leisure', 'healthcare')


# ── Module-level helpers ───────────────────────────────────────────────────────

def _normalise_brand(name: str) -> str:
    """Lower-case, fold Hungarian diacritics, strip everything but letters/digits -
    so 'K&H Bank', 'kh_bank' and 'K&H' all compare equal against EXCLUDED_BRANDS."""
    if not name:
        return ''
    name = name.lower().translate(_DIACRITIC_MAP)
    return re.sub(r'[^a-z0-9]', '', name)


def _is_excluded_brand(brand_name: str) -> bool:
    normalised = _normalise_brand(brand_name)
    if not normalised:
        return False
    return any(normalised == excluded or excluded in normalised or normalised in excluded
              for excluded in EXCLUDED_BRANDS)


def _resolve_feature_type(props: dict):
    """Return (poi_type, tag_key, tag_value) for a feature's real OSM category, or
    None if nothing in props matches a type this project supports."""
    for key in _TYPE_KEY_PRIORITY:
        value = props.get(key)
        if not value:
            continue
        poi_type = _OSM_TAG_TO_POI_TYPE.get((key, value))
        if poi_type:
            return poi_type, key, value
        if key == 'shop':
            return 'shop', key, value
    return None


def _spider_type_poi_code(spider_name: str, poi_type: str) -> str:
    """Deterministic 10-char poi_code - see module docstring for why this can't be
    a readable 'huatp_<spider>_<type>' string (POI_common.poi_code is Unicode(10))."""
    digest = hashlib.sha1('{}:{}'.format(spider_name, poi_type).encode('utf-8')).hexdigest()
    return 'atp{}'.format(digest[:7])


def _is_hu_feature(props: dict, coords: list) -> bool:
    country = props.get('addr:country') or props.get('@country') or props.get('country') or ''
    if country:
        return country in _HU_COUNTRY_CODES
    if len(coords) >= 2:
        lon, lat = float(coords[0]), float(coords[1])
        return _HU_LAT_MIN <= lat <= _HU_LAT_MAX and _HU_LON_MIN <= lon <= _HU_LON_MAX
    return False


# ── Main importer class ────────────────────────────────────────────────────────

class hu_alltheplaces(DataProvider):
    """
    Imports Hungarian POIs from AllThePlaces (alltheplaces.xyz) for every brand that
    doesn't already have a dedicated provider in this project.

    Workflow
    --------
    1. ``contains()`` - sets the primary download URL (latest run JSON).
    2. ``types()``    - downloads run metadata and the insights file; discovers
                        HU-relevant, not-already-covered spiders; downloads each
                        one's GeoJSON, resolves each feature's real type, and
                        registers one poi_common type per (spider, type) pair
                        actually found. Features are cached in memory
                        (self._spider_features) so process() doesn't re-fetch them.
    3. ``process()``  - replays the cached features into the internal POI dataset.
    """

    def contains(self):
        self.link = ATP_LATEST_URL
        self.tags = {'source': 'alltheplaces.xyz'}
        self.filetype = FileType.json
        self.filename = 'hu_alltheplaces_latest.json'
        # {spider_name: [(poi_code, poi_type, feature), ...]} - populated by types(),
        # consumed by process().
        self._spider_features = {}

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _download_latest_run(self):
        content = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename), FileType.json)
        if content is None:
            logging.error('[hu_alltheplaces] Cannot download latest run info from %s', self.link)
            return None
        try:
            return json.loads(content, strict=False)
        except (json.JSONDecodeError, ValueError) as e:
            logging.error('[hu_alltheplaces] Invalid JSON in latest run response: %s', e)
            return None

    def _download_insights(self, insights_url):
        content = save_downloaded_soup(
            insights_url, os.path.join(self.download_cache, 'hu_alltheplaces_insights.json'), FileType.json)
        if content is None:
            logging.error('[hu_alltheplaces] Cannot download insights from %s', insights_url)
            return None
        try:
            return json.loads(content, strict=False)
        except (json.JSONDecodeError, ValueError) as e:
            logging.error('[hu_alltheplaces] Invalid JSON in insights file: %s', e)
            return None

    def _extract_hu_spiders(self, insights: dict) -> dict:
        """Return {spider_name: brand_name} for HU-relevant, not-already-covered spiders."""
        hu_spiders = {}
        data = insights.get('data', [])
        if not isinstance(data, list):
            logging.error('[hu_alltheplaces] Unexpected insights format: "data" is not a list.')
            return hu_spiders

        for brand in data:
            brand_name = brand.get('nsi_brand') or brand.get('q_title') or ''
            if _is_excluded_brand(brand_name):
                continue
            atp_splits = brand.get('atp_splits') or {}
            hu_entries = atp_splits.get('HU') or {}
            if not isinstance(hu_entries, dict):
                continue
            for spider_name in hu_entries:
                if spider_name and spider_name not in hu_spiders:
                    hu_spiders[spider_name] = brand_name

        logging.info('[hu_alltheplaces] %d HU-relevant, not-already-covered spiders found.', len(hu_spiders))
        return hu_spiders

    def _fetch_spider_geojson(self, spider_name):
        spider_url = ATP_SPIDER_OUTPUT_URL.format(spider_name)
        cache_file = os.path.join(self.download_cache, 'hu_alltheplaces_{}.json'.format(spider_name))
        content = save_downloaded_soup(spider_url, cache_file, FileType.json)
        if content is None:
            return None
        try:
            data = json.loads(content, strict=False)
        except (json.JSONDecodeError, ValueError):
            logging.warning('[hu_alltheplaces] Unparseable GeoJSON for spider %s.', spider_name)
            return None
        if isinstance(data, dict) and data.get('type') == 'FeatureCollection':
            return data.get('features') or []
        if isinstance(data, dict) and data.get('type') == 'Feature':
            return [data]
        return None

    # ── types ─────────────────────────────────────────────────────────────────

    def types(self) -> list:
        run_meta = self._download_latest_run()
        if run_meta is None:
            return []
        insights_url = run_meta.get('insights_url', '')
        if not insights_url:
            logging.error('[hu_alltheplaces] Missing insights_url in latest.json: %s', run_meta)
            return []

        insights = self._download_insights(insights_url)
        if insights is None:
            return []

        hu_spiders = self._extract_hu_spiders(insights)

        type_defs = {}  # poi_code -> type dict
        for spider_name, brand_name in hu_spiders.items():
            try:
                features = self._fetch_spider_geojson(spider_name)
            except Exception as e:
                logging.warning('[hu_alltheplaces] Failed to fetch spider %s: %s', spider_name, e)
                continue
            if not features:
                continue

            for feature in features:
                props = feature.get('properties') or {}
                geometry = feature.get('geometry') or {}
                if geometry.get('type') != 'Point':
                    continue
                coords = geometry.get('coordinates') or []
                if len(coords) < 2 or not _is_hu_feature(props, coords):
                    continue
                resolved = _resolve_feature_type(props)
                if resolved is None:
                    continue
                poi_type, tag_key, tag_value = resolved
                poi_code = _spider_type_poi_code(spider_name, poi_type)
                self._spider_features.setdefault(poi_code, []).append(feature)

                if poi_code not in type_defs:
                    common_name = brand_name or spider_name
                    poi_tags = {'source': 'alltheplaces.xyz', 'source:spider': spider_name,
                               'source:osm_tag': '{}={}'.format(tag_key, tag_value)}
                    if brand_name:
                        poi_tags['brand'] = brand_name
                    type_defs[poi_code] = {
                        'poi_code': poi_code,
                        'poi_common_name': '{} ({})'.format(common_name, poi_type),
                        'poi_type': poi_type,
                        'poi_tags': poi_tags,
                        'poi_url_base': 'https://alltheplaces.xyz',
                        'poi_search_name': (brand_name or spider_name).lower(),
                        'osm_search_distance_perfect': 200,
                        'osm_search_distance_safe': 100,
                        'osm_search_distance_unsafe': 20,
                    }

        logging.info('[hu_alltheplaces] Registered %d poi types from %d spiders.', len(type_defs), len(hu_spiders))
        return list(type_defs.values())

    # ── process ───────────────────────────────────────────────────────────────

    def process(self):
        for poi_code, features in self._spider_features.items():
            for feature in features:
                try:
                    self._import_feature(feature, poi_code)
                except Exception as e:
                    logging.error('[hu_alltheplaces] Error importing a %s feature: %s', poi_code, e)
                    logging.exception('Exception occurred')

    def _import_feature(self, feature: dict, poi_code: str) -> bool:
        props = feature.get('properties') or {}
        geometry = feature.get('geometry') or {}
        coords = geometry.get('coordinates') or []
        if len(coords) < 2:
            return False

        # GeoJSON coordinates are [longitude, latitude]; check_hu_boundary(lat, lon)
        lat, lon = check_hu_boundary(float(coords[1]), float(coords[0]))
        if lat is None or lon is None:
            return False

        self.data.code = poi_code
        self.data.lat = lat
        self.data.lon = lon
        self.data.branch = clean_string(props.get('name', '') or props.get('brand', '') or '')

        addr_street = clean_string(props.get('addr:street', ''))
        addr_housenumber = clean_string(props.get('addr:housenumber', ''))
        addr_combined = clean_string(props.get('addr:street_address', '') or props.get('addr:full', ''))

        if addr_street:
            self.data.street = addr_street
            self.data.housenumber = addr_housenumber
            self.data.original = '{} {}'.format(addr_street, addr_housenumber or '').strip()
        elif addr_combined:
            self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                extract_street_housenumber_better_2(addr_combined)
            self.data.original = addr_combined

        self.data.city = clean_city(clean_string(props.get('addr:city', '') or ''))
        self.data.postcode = clean_string(props.get('addr:postcode', '') or '')
        self.data.phone = clean_string(props.get('phone', '') or props.get('contact:phone', '') or '')
        self.data.email = clean_string(props.get('email', '') or props.get('contact:email', '') or '')
        self.data.website = clean_string(
            props.get('website', '') or props.get('contact:website', '') or props.get('@source_uri', '') or '')
        self.data.ref = clean_string(props.get('ref', '') or props.get('@id', '') or '')

        oh = clean_string(props.get('opening_hours', '') or '')
        if oh:
            self.data.opening_hours = oh

        self.data.public_holiday_open = False
        self.data.add()
        return True
