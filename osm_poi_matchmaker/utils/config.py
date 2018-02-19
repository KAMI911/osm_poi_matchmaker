# -*- coding: utf-8 -*-

try:
    import traceback
    import configparser
    import logging
    import sys
    import os
    from enum import Enum
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

config = configparser.ConfigParser()
config.sections()
config.read("app.conf")


class Mode(Enum):
    matcher = 0
    server = 1


__mode = Mode.matcher


def set_mode(mode):
    if not isinstance(mode, Mode):
        raise ValueError("Cannot set mode to {}".format(mode))

    global __mode
    __mode = mode


if not config.has_section(__mode.name):
    logging.fatal("Config section missing for server {}".format(__mode.name))
    sys.exit(-1)

currentConfig = config[__mode.name]

KEY_DIRECTORY_OUTPUT = 'dir.output'
KEY_DIRECTORY_CACHE_URL = 'dir.cache_url'
KEY_DATABASE_TYPE = 'db.type'
KEY_DATABASE_WRITE_HOST = 'db.write.host'
KEY_DATABASE_WRITE_PORT = 'db.write.port'
KEY_DATABASE_WRITE_USERNAME = 'db.write.username'
KEY_DATABASE_WRITE_PASSWORD = 'db.write.password'
KEY_GEO_DEFAULT_PROJECTION = 'geo.default.projection'
KEY_DOWNLOAD_VERIFY_LINK = 'download.verify.link'
KEY_DOWNLOAD_USE_CACHED_DATA = 'download.use.cached.data'

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


def init_log():
    logging.config.fileConfig("log.conf")


def get_directory_output():
    setting = get_config_string(KEY_DIRECTORY_OUTPUT)
    if None != setting:
        return setting
    else:
        return '.'


def get_directory_cache_url():
    setting = get_config_string(KEY_DIRECTORY_CACHE_URL)
    if None != setting:
        return setting
    else:
        return os.path.join('.', 'cache_url')


def get_database_type():
    setting = get_config_string(KEY_DATABASE_TYPE)
    if None != setting:
        return setting
    else:
        return os.path.join('postgresql')


def get_database_writer_host():
    setting = get_config_string(KEY_DATABASE_WRITE_HOST)
    if None != setting:
        logging.info('Using "{}" for database host.'.format(setting))
        return setting
    else:
        logging.info('Using localhost for database host.')
        return os.path.join('localhost')


def get_database_writer_port():
    setting = get_config_int(KEY_DATABASE_WRITE_PORT)
    if None != setting:
        return setting
    else:
        return os.path.join('5432')


def get_database_writer_username():
    setting = get_config_string(KEY_DATABASE_WRITE_USERNAME)
    if setting == 'poi':
        logging.warning(
            'Using default username. For security concerns please change default username in the config file and the database.')
        if None != setting:
            return setting
    else:
        return os.path.join('poi')


def get_database_writer_password():
    setting = get_config_string(KEY_DATABASE_WRITE_PASSWORD)
    if setting == 'poitest':
        logging.warning(
            'Using default password. For security concerns please change default password in the config file and the database.')
    if None != setting:
        return setting
    else:
        return os.path.join('poitest')


def get_geo_default_projection():
    setting = get_config_int(KEY_GEO_DEFAULT_PROJECTION)
    if None != setting:
        return setting
    else:
        return '4326'


def get_download_verify_link():
    setting = get_config_bool(KEY_DOWNLOAD_VERIFY_LINK)
    if None != setting:
        return setting
    else:
        return True

def get_download_use_cached_data():
    setting = get_config_bool(KEY_DOWNLOAD_USE_CACHED_DATA)
    if None != setting:
        return setting
    else:
        return True
