# -*- coding: utf-8 -*-

try:
    import logging
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
    matcher = 0
    server = 1


__mode = Mode.matcher


def set_mode(mode):
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
KEY_GEO_DEFAULT_PROJECTION = 'geo.default.projection'
KEY_GEO_DEFAULT_POI_DISTANCE = 'geo.default.poi.distance'
KEY_GEO_DEFAULT_POI_UNSAFE_DISTANCE = 'geo.default.poi.unsafe.distance'
KEY_GEO_DEFAULT_POI_PERFECT_DISTANCE = 'geo.default.poi.perfect.distance'
KEY_GEO_DEFAULT_POI_ROAD_DISTANCE = 'geo.default.poi.road.distance'
KEY_GEO_AMENITY_ATM_POI_DISTANCE = 'geo.amenity.atm.poi.distance'
KEY_GEO_SHOP_CONVENIENCE_POI_DISTANCE = 'geo.shop.convenience.poi.distance'
KEY_GEO_AMENITY_POST_OFFICE_POI_DISTANCE = 'geo.amenity.post.office.poi.distance'
KEY_GEO_PREFER_OSM_POSTCODE = 'geo.prefer.osm.postcode'
KEY_GEO_ALTERNATIVE_OPENING_HOURS = 'geo.alternative.opening.hours'
KEY_GEO_ALTERNATIVE_OPENING_HOURS_TAG = 'geo.alternative.opening.hours.tag'
KEY_DOWNLOAD_VERIFY_LINK = 'download.verify.link'
KEY_DOWNLOAD_USE_CACHED_DATA = 'download.use.cached.data'
KEY_USE_GENERAL_SOURCE_WEBSITE_DATE = 'use.general.source.website.date'
KEY_USE_GENERAL_SOURCE_WEBSITE_DATE_TAG = 'use.general.source.website.date.tag'
KEY_DATAPROVIDERS_MODULES_AVAILABLE = 'dataproviders.modules.available'
KEY_DATAPROVIDERS_MODULES_ENABLE = 'dataproviders.modules.enable'
KEY_DATAPROVIDERS_LIMIT_ELEMENTS = 'dataproviders.limit.elements'


def get_config(key):
    if key in currentConfig:
        return currentConfig[key]
    else:
        return None


def get_config_bool(key):
    return config.getboolean(__mode.name, key)


def get_config_int(key):
    return config.getint(__mode.name, key)


def get_config_string(key):
    return config.get(__mode.name, key)


def get_config_list(key):
    return config.get(__mode.name, key).split(',')


def init_log():
    logging.config.fileConfig("log.conf")


def get_directory_output():
    setting = get_config_string(KEY_DIRECTORY_OUTPUT)
    env_setting = os.environ.get('OPM_DIRECTORY_OUTPUT')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return '.'


def get_directory_cache_url():
    setting = get_config_string(KEY_DIRECTORY_CACHE_URL)
    env_setting = os.environ.get('OPM_DIRECTORY_CACHE_URL')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return os.path.join('.', 'cache_url')


def get_database_type():
    setting = get_config_string(KEY_DATABASE_TYPE)
    env_setting = os.environ.get('OPM_DATABASE_TYPE')
    if env_setting:
        return env_setting
    if setting is not None:
        return setting
    else:
        return 'postgresql'


def get_database_writer_host():
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
    setting = get_config_int(KEY_DATABASE_WRITE_PORT)
    env_setting = os.environ.get('OPM_DATABASE_WRITE_PORT')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return '5432'


def get_database_writer_username():
    setting = get_config_string(KEY_DATABASE_WRITE_USERNAME)
    env_setting = os.environ.get('OPM_DATABASE_WRITE_USERNAME')
    if env_setting is not None:
        setting = env_setting
    if setting == 'poi':
        logging.warning(
            'Using default username. For security concerns please change default username in the config file and the database.')
        if setting is not None:
            return setting
    else:
        return 'poi'


def get_database_writer_password():
    setting = get_config_string(KEY_DATABASE_WRITE_PASSWORD)
    env_setting = os.environ.get('OPM_DATABASE_WRITE_PASSWORD')
    if env_setting is not None:
        setting = env_setting
    if setting == 'poitest':
        logging.warning(
            'Using default password. For security concerns please change default password in the config file and the database.')
    if None != setting:
        return setting
    else:
        return 'poitest'


def get_database_poi_database():
    setting = get_config_string(KEY_DATABASE_POI_DATABASE)
    env_setting = os.environ.get('OPM_DATABASE_POI_DATABASE')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return 'poi'


def get_database_enable_query_log():
    setting = get_config_bool(KEY_DATABASE_ENABLE_QUERY_LOG)
    if setting is not None:
        return setting
    else:
        return False


def get_database_enable_analyze():
    setting = get_config_bool(KEY_DATABASE_ENABLE_ANALYZE)
    if setting is not None:
        return setting
    else:
        return False


def get_database_enable_huge_query():
    setting = get_config_bool(KEY_DATABASE_ENABLE_HUGE_QUERY)
    if setting is not None:
        return setting
    else:
        return False
    
    
def get_database_start_drop_poi_tables():
    setting = get_config_bool(KEY_DATABASE_START_DROP_POI_TABLES)
    if setting is not None:
        return setting
    else:
        return True


def get_geo_default_projection():
    setting = get_config_int(KEY_GEO_DEFAULT_PROJECTION)
    if setting is not None:
        return setting
    else:
        return 4326


def get_geo_default_poi_distance():
    setting = get_config_int(KEY_GEO_DEFAULT_POI_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 70


def get_geo_default_poi_unsafe_distance():
    setting = get_config_int(KEY_GEO_DEFAULT_POI_UNSAFE_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 5


def get_geo_default_poi_perfect_distance():
    setting = get_config_int(KEY_GEO_DEFAULT_POI_PERFECT_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 300


def get_geo_default_poi_road_distance():
    setting = get_config_int(KEY_GEO_DEFAULT_POI_ROAD_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 600


def get_geo_amenity_atm_poi_distance():
    setting = get_config_int(KEY_GEO_AMENITY_ATM_POI_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 20


def get_geo_shop_poi_distance():
    setting = get_config_int(KEY_GEO_SHOP_CONVENIENCE_POI_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 50


def get_geo_amenity_post_office_poi_distance():
    setting = get_config_int(KEY_GEO_AMENITY_POST_OFFICE_POI_DISTANCE)
    if setting is not None:
        return setting
    else:
        return 250


def get_geo_prefer_osm_postcode():
    setting = get_config_bool(KEY_GEO_PREFER_OSM_POSTCODE)
    if setting is not None:
        return setting
    else:
        return True


def get_geo_alternative_opening_hours():
    setting = get_config_bool(KEY_GEO_ALTERNATIVE_OPENING_HOURS)
    if setting is not None:
        return setting
    else:
        return False


def get_geo_alternative_opening_hours_tag():
    setting = get_config_string(KEY_GEO_ALTERNATIVE_OPENING_HOURS_TAG)
    if setting is not None:
        return setting
    else:
        return None


def get_download_verify_link():
    setting = get_config_bool(KEY_DOWNLOAD_VERIFY_LINK)
    if setting is not None:
        return setting
    else:
        return True


def get_download_use_cached_data():
    setting = get_config_bool(KEY_DOWNLOAD_USE_CACHED_DATA)
    if setting is not None:
        return setting
    else:
        return True


def get_use_general_source_website_date():
    setting = get_config_bool(KEY_USE_GENERAL_SOURCE_WEBSITE_DATE)
    env_setting = os.environ.get('USE_GENERAL_SOURCE_WEBSITE_DATE')
    if env_setting is not None:
        return env_setting
    if setting is not None:
        return setting
    else:
        return True


def get_use_general_source_website_date_tag():
    setting = get_config_string(KEY_USE_GENERAL_SOURCE_WEBSITE_DATE_TAG)
    if setting is not None:
        return setting
    else:
        return 'source:date'


def get_dataproviders_modules_available():
    setting = get_config_list(KEY_DATAPROVIDERS_MODULES_AVAILABLE)
    env_setting = os.environ.get('OPM_DATAPROVIDERS_MODULES_AVAILABLE')
    if env_setting is not None:
        setting = env_setting
    if setting is not None:
        return setting
    else:
        return True


def get_dataproviders_modules_enable():
    setting = get_config_list(KEY_DATAPROVIDERS_MODULES_ENABLE)
    env_setting = os.environ.get('OPM_DATAPROVIDERS_MODULES_ENABLE')
    if env_setting is not None:
        setting = env_setting
    if setting is not None:
        return setting
    else:
        return True

def get_dataproviders_limit_elemets():
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
