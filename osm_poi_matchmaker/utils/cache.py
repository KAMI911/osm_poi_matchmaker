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
    """Read a previously cached value for the given key.

    Args:
        key (str): Arbitrary cache key, e.g. an ETag cache key like 'etag:<url>'.

    Returns:
        dict | str | None: The cached JSON value, or None if nothing is cached for this key.
    """
    file_path = get_cache_path(key)
    if os.path.exists(file_path):
        with open(file_path, mode='r', encoding='utf-8') as file:
            return json.load(file)
    return None


def set_cached(key: str, data: dict | str) -> None:
    """Write a value to the on-disk cache for the given key, creating the cache
    directory if needed.

    Args:
        key (str): Arbitrary cache key.
        data (dict | str): JSON-serializable value to store.
    """
    file_path = get_cache_path(key)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as file:
        json.dump(data, file)


def get_cache_path(key: str) -> str:
    """Compute the on-disk cache file path for a key.

    The key itself is hashed (MD5) so arbitrary keys (URLs, etc.) become safe filenames.

    Args:
        key (str): Arbitrary cache key.

    Returns:
        str: Path under the configured cache directory's 'cache/' subfolder.
    """
    return '{}/cache/{}.cache'.format(config.get_directory_cache_url(), hashlib.md5(key.encode('utf-8')).hexdigest())
