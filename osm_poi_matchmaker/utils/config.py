# -*- coding: utf-8 -*-

try:
    import logging
    import logging.config
    import sys
    import configparser
    import os
    from enum import Enum
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

try:
    config = configparser.ConfigParser(strict=False)
    config.sections()
    config.read('app.conf')
except configparser.DuplicateOptionError as e:
    logging.error('At least one of configured key has multiple value. Please review your app.conf file.')


class Mode(Enum):
    """Which app.conf section to read settings from ([matcher] or [server])."""
    matcher = 0
    server = 1


__mode = Mode.matcher


def set_mode(mode):
    """Switch which app.conf section subsequent get_*() calls read from.

    Args:
        mode (Mode): The section to activate.

    Raises:
        ValueError: If mode is not a Mode enum member.
    """
    if not isinstance(mode, Mode):
        raise ValueError('Cannot set mode to %s', mode)

    global __mode
    __mode = mode


if not config.has_section(__mode.name):
    logging.fatal('sections: %s', config.sections())
    logging.fatal('Config section missing for server %s', __mode.name)
    sys.exit(-1)

currentConfig = config[__mode.name]

KEY_DIRECTORY_OUTPUT = 'dir.output'
KEY_DIRECTORY_CACHE_URL = 'dir.cache_url'
KEY_DATABASE_TYPE = 'db.type'
KEY_DATABASE_WRITE_HOST = 'db.write.host'
KEY_DATABASE_WRITE_PORT = 'db.write.port'
KEY_DATABASE_WRITE_USERNAME = 'db.write.username'
KEY_DATABASE_WRITE_PASSWORD = 'db.write.password'
KEY_DATABASE_POI_DATABASE = 'db.poi.database'
KEY_DATABASE_ENABLE_QUERY_LOG = 'db.enable.query_log'
KEY_DATABASE_ENABLE_ANALYZE = 'db.enable.analyze'
KEY_DATABASE_ENABLE_HUGE_QUERY = 'db.enable.huge_query'
KEY_DATABASE_START_DROP_POI_TABLES = 'db.start.drop.poi_tables'
KEY_DATABASE_OSM_CACHE_MAX_AGE_DAYS = 'db.osm_cache.max_age_days'
KEY_GEO_DEFAULT_PROJECTION = 'geo.default.projection'
KEY_GEO_DEFAULT_POI_DISTANCE = 'geo.default.poi.distance'
KEY_GEO_DEFAULT_POI_UNSAFE_DISTANCE = 'geo.default.poi.unsafe.distance'
KEY_GEO_DEFAULT_POI_PERFECT_DISTANCE = 'geo.default.poi.perfect.distance'
KEY_GEO_DEFAULT_POI_ROAD_DISTANCE = 'geo.default.poi.road.distance'
KEY_GEO_DEFAULT_POI_ROAD_TRUST_DISTANCE = 'geo.default.poi.road.trust.distance'
KEY_GEO_AMENITY_ATM_POI_DISTANCE = 'geo.amenity.atm.poi.distance'
KEY_GEO_SHOP_CONVENIENCE_POI_DISTANCE = 'geo.shop.convenience.poi.distance'
KEY_GEO_AMENITY_POST_OFFICE_POI_DISTANCE = 'geo.amenity.post.office.poi.distance'
KEY_GEO_PREFER_OSM_POSTCODE = 'geo.prefer.osm.postcode'
KEY_GEO_PREFER_OSM_ORIGINAL_POSTCODE = 'geo.prefer.osm.original.postcode'
KEY_GEO_ALTERNATIVE_OPENING_HOURS = 'geo.alternative.opening.hours'
KEY_GEO_ALTERNATIVE_OPENING_HOURS_TAG = 'geo.alternative.opening.hours.tag'
KEY_DOWNLOAD_VERIFY_LINK = 'download.verify.link'
KEY_DOWNLOAD_USE_CACHED_DATA = 'download.use.cached.data'
KEY_USE_GENERAL_SOURCE_WEBSITE_DATE = 'use.general.source.website.date'
KEY_USE_GENERAL_SOURCE_WEBSITE_DATE_TAG = 'use.general.source.website.date.tag'
KEY_DATAPROVIDERS_MODULES_AVAILABLE = 'dataproviders.modules.available'
KEY_DATAPROVIDERS_MODULES_ENABLE = 'dataproviders.modules.enable'
KEY_DATAPROVIDERS_LIMIT_ELEMENTS = 'dataproviders.limit.elements'
KEY_DATAPROVIDERS_MAV_GTFS_USERNAME = 'dataproviders.mav.gtfs.username'
KEY_DATAPROVIDERS_MAV_GTFS_PASSWORD = 'dataproviders.mav.gtfs.password'
KEY_MEMORY_ENABLE_TRACING = 'memory.enable.tracing'
KEY_LOG_LEVEL = 'log.level'


def get_config(key):
    """Return the raw string value of a key from the active app.conf section.

    Args:
        key (str): One of the KEY_* config key constants.

    Returns:
        str | None: The raw config value, or None if the key is missing.
    """
    if key in currentConfig:
        return currentConfig[key]
    else:
        return None


def get_config_bool(key):
    """Return a key from the active app.conf section, parsed as a boolean."""
    return config.getboolean(__mode.name, key)


def get_config_int(key):
    """Return a key from the active app.conf section, parsed as an int."""
    return config.getint(__mode.name, key)


def get_config_string(key):
    """Return a key from the active app.conf section as a raw string."""
    return config.get(__mode.name, key)


def get_config_list(key):
    """Return a key from the active app.conf section, split on commas into a list."""
    return config.get(__mode.name, key).split(',')


def get_log_level():
    """Root logger/handler level, overriding whatever log.conf itself sets.

    Config key 'log.level' (env OPM_LOG_LEVEL), defaults to 'INFO'. Accepts the
    standard logging level names (DEBUG, INFO, WARNING, ERROR, CRITICAL) - lets
    you switch verbosity from app.conf without editing log.conf.
    """
    setting = get_config(KEY_LOG_LEVEL)
    env_setting = os.environ.get('OPM_LOG_LEVEL')
    if env_setting is not None:
        return env_setting.upper()
    if setting is not None:
        return setting.upper()
    else:
        return 'INFO'


def init_log():
    """Configure the logging module from log.conf (fileConfig-style).

    Also attaches ProviderLogFilter to every root handler - see log.conf's
    %(provider)s and utils/log_context.py - so log lines can be tagged with
    the data provider currently being harvested. This has to be a HANDLER
    filter, not a root LOGGER filter: a Logger's own filters only run for
    records logged directly through that logger, never for records from
    another logger (e.g. sqlalchemy.engine.Engine, enabled by
    db.enable.query_log) that merely propagate up to the root handler - those
    would hit %(provider)s with no record.provider set and crash the handler
    with "KeyError: 'provider'". A handler's filters run for every record
    that reaches it, regardless of which logger it came from.

    The root logger's and its handlers' level is then overridden from
    get_log_level() (app.conf's 'log.level') - log.conf's own level=... lines
    still apply as the fallback default if that key isn't set."""
    from osm_poi_matchmaker.utils.log_context import ProviderLogFilter
    logging.config.fileConfig("log.conf")
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(ProviderLogFilter())
    level = get_log_level()
    root_logger.setLevel(level)
    for handler in root_logger.handlers:
        handler.setLevel(level)


# The get_*() functions below all follow the same pattern: read the setting from
# app.conf, let an OPM_* environment variable override it if set, and fall back to
# a hardcoded default if neither is present. Each docstring below states the app.conf
# key and the fallback default; the corresponding OPM_<NAME> env var (where supported)
# always takes precedence over both.

def get_directory_output():
    """Output directory for generated OSM/GeoJSON/CSV files.

    Config key 'dir.output' (env OPM_DIRECTORY_OUTPUT), defaults to '.'.
    """
    setting = get_config_string(KEY_DIRECTORY_OUTPUT)
    env_setting = os.environ.get('OPM_DIRECTORY_OUTPUT')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return '.'


def get_directory_cache_url():
    """Directory where downloaded source files and cache files are stored.

    Config key 'dir.cache_url' (env OPM_DIRECTORY_CACHE_URL), defaults to './cache_url'.
    """
    setting = get_config_string(KEY_DIRECTORY_CACHE_URL)
    env_setting = os.environ.get('OPM_DIRECTORY_CACHE_URL')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return os.path.join('.', 'cache_url')


def get_database_type():
    """SQLAlchemy dialect name for the POI database.

    Config key 'db.type' (env OPM_DATABASE_TYPE), defaults to 'postgresql'.
    """
    setting = get_config_string(KEY_DATABASE_TYPE)
    env_setting = os.environ.get('OPM_DATABASE_TYPE')
    if env_setting:
        return env_setting
    if setting is not None:
        return setting
    else:
        return 'postgresql'


def get_database_writer_host():
    """Hostname of the writable POI database.

    Config key 'db.write.host' (env OPM_DATABASE_WRITE_HOST), defaults to 'localhost'.
    """
    setting = get_config_string(KEY_DATABASE_WRITE_HOST)
    env_setting = os.environ.get('OPM_DATABASE_WRITE_HOST')
    if env_setting is not None:
        setting = env_setting
    if setting is not None:
        logging.info('Using "%s" for database host.', setting)
        return setting
    else:
        logging.info('Using localhost for database host.')
        return 'localhost'


def get_database_writer_port():
    """Port of the writable POI database.

    Config key 'db.write.port' (env OPM_DATABASE_WRITE_PORT), defaults to '5432'.
    """
    setting = get_config_int(KEY_DATABASE_WRITE_PORT)
    env_setting = os.environ.get('OPM_DATABASE_WRITE_PORT')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return '5432'


def get_database_writer_username():
    """Username for the writable POI database connection.

    Config key 'db.write.username' (env OPM_DATABASE_WRITE_USERNAME), defaults to
    'poi' (and logs a warning, since that's an insecure default).
    """
    setting = get_config_string(KEY_DATABASE_WRITE_USERNAME)
    env_setting = os.environ.get('OPM_DATABASE_WRITE_USERNAME')
    if env_setting is not None:
        setting = env_setting
    if setting is None:
        return 'poi'
    if setting == 'poi':
        logging.warning(
            'Using default username. For security concerns please change default username in the config file and the database.')
    return setting


def get_database_writer_password():
    """Password for the writable POI database connection.

    Config key 'db.write.password' (env OPM_DATABASE_WRITE_PASSWORD), defaults to
    'poitest' (and logs a warning, since that's an insecure default).
    """
    setting = get_config_string(KEY_DATABASE_WRITE_PASSWORD)
    env_setting = os.environ.get('OPM_DATABASE_WRITE_PASSWORD')
    if env_setting is not None:
        setting = env_setting
    if setting == 'poitest':
        logging.warning(
            'Using default password. For security concerns please change default password in the config file and the database.')
    if setting is not None:
        return setting
    else:
        return 'poitest'


def get_mav_gtfs_username():
    """Username for MÁV's HTTP Basic Auth-protected GTFS feed (hu_mav.py).

    Config key 'dataproviders.mav.gtfs.username' (env OPM_MAV_GTFS_USERNAME). Unlike the
    database credentials there's no sane default - this is a third-party credential, not
    checked into git (only the untracked local app.conf, never app.conf-template). Returns
    None if unset, so hu_mav.py can fall back to a manually-provided local GTFS file.
    """
    env_setting = os.environ.get('OPM_MAV_GTFS_USERNAME')
    if env_setting is not None:
        return env_setting
    return get_config(KEY_DATAPROVIDERS_MAV_GTFS_USERNAME)


def get_mav_gtfs_password():
    """Password for MÁV's HTTP Basic Auth-protected GTFS feed (hu_mav.py).

    Config key 'dataproviders.mav.gtfs.password' (env OPM_MAV_GTFS_PASSWORD). See
    get_mav_gtfs_username() for why there's no default and where this belongs (or doesn't).
    """
    env_setting = os.environ.get('OPM_MAV_GTFS_PASSWORD')
    if env_setting is not None:
        return env_setting
    return get_config(KEY_DATAPROVIDERS_MAV_GTFS_PASSWORD)


def get_database_poi_database():
    """Database name holding the POI tables.

    Config key 'db.poi.database' (env OPM_DATABASE_POI_DATABASE), defaults to 'poi'.
    """
    setting = get_config_string(KEY_DATABASE_POI_DATABASE)
    env_setting = os.environ.get('OPM_DATABASE_POI_DATABASE')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return 'poi'


def get_database_enable_query_log():
    """Whether SQLAlchemy should echo every SQL statement it runs.

    Config key 'db.enable.query_log', defaults to False.
    """
    setting = get_config_bool(KEY_DATABASE_ENABLE_QUERY_LOG)
    if setting is not None:
        return setting
    else:
        return False


def get_database_enable_analyze():
    """Whether to run ANALYZE on POI tables after bulk changes.

    Config key 'db.enable.analyze', defaults to False.
    """
    setting = get_config_bool(KEY_DATABASE_ENABLE_ANALYZE)
    if setting is not None:
        return setting
    else:
        return False


def get_database_enable_huge_query():
    """Whether to combine each matching stage's per-priority-level candidate queries
    into a single UNION ALL query instead of running them one at a time. Measured
    ~1.9x faster on this project's data (fewer round trips outweighs the cost of a
    larger query) - see issue #100.

    Config key 'db.enable.huge_query', defaults to True.
    """
    setting = get_config_bool(KEY_DATABASE_ENABLE_HUGE_QUERY)
    if setting is not None:
        return setting
    else:
        return True


def get_database_start_drop_poi_tables():
    """Whether to drop and recreate the POI tables at the start of a run.

    Config key 'db.start.drop.poi_tables', defaults to True.
    """
    setting = get_config_bool(KEY_DATABASE_START_DROP_POI_TABLES)
    if setting is not None:
        return setting
    else:
        return True


def get_database_osm_cache_max_age_days():
    """Age in days after which a poi_osm_cache row (a live OSM node/way/relation
    lookup, see dao/data_structure.py's POI_OSM_cache) is treated as stale and
    re-fetched from the live API instead of reused as-is.

    poi_osm_cache is explicitly meant to survive across runs (so the matcher
    doesn't re-fetch the same element on every run) - with
    db.start.drop.poi_tables=True (the default) it gets wiped at the start of
    each run anyway, but a run with that off keeps accumulating entries with no
    other staleness check at all, so a row cached weeks or months ago would be
    trusted forever even after the real OSM element moved, got retagged, or was
    deleted by someone else in the meantime.

    Config key 'db.osm_cache.max_age_days', defaults to 7.
    """
    setting = get_config_int(KEY_DATABASE_OSM_CACHE_MAX_AGE_DAYS)
    if setting is not None:
        return setting
    else:
        return 7


def get_geo_default_projection():
    """EPSG code used for POI geometries.

    Config key 'geo.default.projection', defaults to 4326 (WGS 84).
    """
    setting = get_config_int(KEY_GEO_DEFAULT_PROJECTION)
    if setting is not None:
        return setting
    else:
        return 4326


def get_geo_default_poi_distance():
    """Fallback POI search distance in meters, used when a provider's types() doesn't
    specify osm_search_distance_* for a given POI type.

    Config key 'geo.default.poi.distance', defaults to 70.
    """
    setting = get_config_int(KEY_GEO_DEFAULT_POI_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 70


def get_geo_default_poi_unsafe_distance():
    """Fallback 'unsafe' match distance in meters (see osm_search_distance_unsafe).

    Config key 'geo.default.poi.unsafe.distance', defaults to 5.
    """
    setting = get_config_int(KEY_GEO_DEFAULT_POI_UNSAFE_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 5


def get_geo_default_poi_perfect_distance():
    """Fallback 'perfect match' distance in meters (see osm_search_distance_perfect).

    Config key 'geo.default.poi.perfect.distance', defaults to 300.
    """
    setting = get_config_int(KEY_GEO_DEFAULT_POI_PERFECT_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 300


def get_geo_default_poi_road_distance():
    """Fallback distance in meters used when matching a POI against nearby roads.

    Config key 'geo.default.poi.road.distance', defaults to 600.
    """
    setting = get_config_int(KEY_GEO_DEFAULT_POI_ROAD_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 600


def get_geo_default_poi_road_trust_distance():
    """Distance in meters within which the single nearest named OSM road is adopted
    as addr:street even without any name similarity to the source address - e.g. a
    source street of 'Rákóczi út' vs. the real nearby road 'Rákóczi Ferenc utca' (too
    dissimilar for query_name_road_around()'s similarity/metaphone/levenshtein checks
    to catch). At this short a range, proximity itself is the reliable signal - name
    text isn't, since Hungary has hundreds of unrelated streets sharing a common first
    word (surnames like 'Szabó', 'Kovács'; generic 'Szent', 'Nagy', 'Dr.', ...).

    Config key 'geo.default.poi.road.trust.distance', defaults to 30.
    """
    setting = get_config_int(KEY_GEO_DEFAULT_POI_ROAD_TRUST_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 30


def get_geo_amenity_atm_poi_distance():
    """Default search distance in meters for amenity=atm POIs.

    Config key 'geo.amenity.atm.poi.distance', defaults to 20.
    """
    setting = get_config_int(KEY_GEO_AMENITY_ATM_POI_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 20


def get_geo_shop_poi_distance():
    """Default search distance in meters for shop=convenience/supermarket POIs.

    Config key 'geo.shop.convenience.poi.distance', defaults to 50.
    """
    setting = get_config_int(KEY_GEO_SHOP_CONVENIENCE_POI_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 50


def get_geo_amenity_post_office_poi_distance():
    """Default search distance in meters for amenity=post_office POIs.

    Config key 'geo.amenity.post.office.poi.distance', defaults to 250.
    """
    setting = get_config_int(KEY_GEO_AMENITY_POST_OFFICE_POI_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 250


def get_geo_prefer_osm_postcode():
    """Whether to prefer OSM's own boundary=postal_code polygon lookup over the
    data provider's postcode when both have a candidate and neither is already
    tagged on the matched element itself (see get_geo_prefer_osm_original_
    postcode() for that, higher-priority case).

    Defaults to True. Note: OSM's postal_code boundary polygons in Hungary are
    hand-drawn/community-maintained, not an authoritative dataset, and can be
    imprecise near their edges - confirmed live incorrectly overriding a correct
    provider-supplied postcode with a neighbouring zone's (see 4ac508e). Setting
    this to False prefers the provider's own postcode instead whenever it has
    one; query_postcode_osm_external() falls back to the other source either way
    if its preferred one has nothing, so no candidate is ever silently dropped
    regardless of this setting.

    Config key 'geo.prefer.osm.postcode'.
    """
    setting = get_config_bool(KEY_GEO_PREFER_OSM_POSTCODE)
    if setting is not None:
        return setting
    else:
        return True


def get_geo_prefer_osm_original_postcode():
    """Whether to trust a postcode already tagged directly on the matched OSM
    element itself (addr:postcode on that exact node/way) over everything else -
    the data provider's postcode and the boundary=postal_code polygon lookup
    both included.

    Defaults to True: a postcode already present on the specific POI being
    matched is normally a human-verified, precise value - a stronger signal than
    either the provider's own data or a geometric polygon-membership guess, so it
    should win whenever it exists. See get_geo_prefer_osm_postcode() for the
    separate setting controlling the fallback order when this one has nothing.

    Config key 'geo.prefer.osm.original.postcode'.
    """
    setting = get_config_bool(KEY_GEO_PREFER_OSM_ORIGINAL_POSTCODE)
    if setting is not None:
        return setting
    else:
        return True


def get_geo_alternative_opening_hours():
    """Whether to write proposed opening_hours changes to a separate tag instead of
    overwriting the existing opening_hours tag (see generate_osm_xml()).

    Config key 'geo.alternative.opening.hours', defaults to False.
    """
    setting = get_config_bool(KEY_GEO_ALTERNATIVE_OPENING_HOURS)
    if setting is not None:
        return setting
    else:
        return False


def get_geo_alternative_opening_hours_tag():
    """Tag name to use for the alternative opening-hours value (see
    get_geo_alternative_opening_hours()).

    Config key 'geo.alternative.opening.hours.tag', defaults to None.
    """
    setting = get_config_string(KEY_GEO_ALTERNATIVE_OPENING_HOURS_TAG)
    if setting is not None:
        return setting
    else:
        return None


def get_download_verify_link():
    """Whether to verify TLS certificates when downloading source files.

    Config key 'download.verify.link', defaults to True.
    """
    setting = get_config_bool(KEY_DOWNLOAD_VERIFY_LINK)
    if setting is not None:
        return setting
    else:
        return True


def get_download_use_cached_data():
    """Whether save_downloaded_soup() may skip re-downloading if a cached copy exists.

    Config key 'download.use.cached.data', defaults to True.
    """
    setting = get_config_bool(KEY_DOWNLOAD_USE_CACHED_DATA)
    if setting is not None:
        return setting
    else:
        return True


def get_use_general_source_website_date():
    """Whether to write a single shared 'source:date' tag instead of one
    'source:<domain>:date' tag per provider.

    Config key 'use.general.source.website.date' (env USE_GENERAL_SOURCE_WEBSITE_DATE),
    defaults to True.
    """
    setting = get_config_bool(KEY_USE_GENERAL_SOURCE_WEBSITE_DATE)
    env_setting = os.environ.get('USE_GENERAL_SOURCE_WEBSITE_DATE')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return True


def get_use_general_source_website_date_tag():
    """Tag name used for the shared source-date tag (see
    get_use_general_source_website_date()).

    Config key 'use.general.source.website.date.tag', defaults to 'source:date'.
    """
    setting = get_config_string(KEY_USE_GENERAL_SOURCE_WEBSITE_DATE_TAG)
    if setting is not None:
        return setting
    else:
        return 'source:date'


def get_dataproviders_modules_available():
    """List of provider module names that exist and can be enabled.

    Config key 'dataproviders.modules.available' (env
    OPM_DATAPROVIDERS_MODULES_AVAILABLE), defaults to True (i.e. unset/no restriction)
    if neither is configured.
    """
    setting = get_config_list(KEY_DATAPROVIDERS_MODULES_AVAILABLE)
    env_setting = os.environ.get('OPM_DATAPROVIDERS_MODULES_AVAILABLE')
    if env_setting is not None:
        setting = env_setting
    if setting is not None:
        return setting
    else:
        return True


def get_dataproviders_modules_enable():
    """List of provider module names that actually run in this pipeline (a subset of
    get_dataproviders_modules_available()).

    Config key 'dataproviders.modules.enable' (env OPM_DATAPROVIDERS_MODULES_ENABLE),
    defaults to True (i.e. unset/no restriction) if neither is configured.
    """
    setting = get_config_list(KEY_DATAPROVIDERS_MODULES_ENABLE)
    env_setting = os.environ.get('OPM_DATAPROVIDERS_MODULES_ENABLE')
    if env_setting is not None:
        setting = env_setting
    if setting is not None:
        return setting
    else:
        return True

def get_dataproviders_limit_elemets():
    """Optional cap on how many items each provider's process() loop handles, for
    quick test runs.

    Config key 'dataproviders.limit.elements' (env OPM_DATAPROVIDERS_LIMIT_ELEMENTS),
    defaults to None (no limit).
    """
    try:
        setting = get_config_int(KEY_DATAPROVIDERS_LIMIT_ELEMENTS)
    except Exception as err:
        setting = None
    env_setting = os.environ.get('OPM_DATAPROVIDERS_LIMIT_ELEMENTS')
    if env_setting is not None:
        setting = env_setting
    if setting is not None:
        logging.info('Setting is not None: {}'.format(setting))
        return setting
    else:
        return None


def get_memory_enable_tracing():
    """Whether MemoryInfo should trace and log memory usage between pipeline stages.

    Config key 'memory.enable.tracing', defaults to False.
    """
    setting = get_config_bool(KEY_MEMORY_ENABLE_TRACING)
    if setting is not None:
        return setting
    else:
        return False
