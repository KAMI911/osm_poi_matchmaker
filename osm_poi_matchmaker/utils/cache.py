# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import json
    import os
    import hashlib
    from enum import Enum
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def get_cached(key: str) -> dict | str | None:
    file_path = get_cache_path(key)
    if os.path.exists(file_path):
        with open(file_path, mode='r', encoding='utf-8') as file:
            return json.load(file)
    return None


def set_cached(key: str, data: dict | str) -> None:
    file_path = get_cache_path(key)
    with open(file_path, "w") as file:
        json.dump(data, file)


def get_cache_path(key: str) -> str:
    return '{}/cache/{}.cache'.format(config.get_directory_cache_url(), hashlib.md5(key.encode('utf-8')).hexdigest())
