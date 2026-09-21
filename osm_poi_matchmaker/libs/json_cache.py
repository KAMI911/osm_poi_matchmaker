# -*- coding: utf-8 -*-

"""Shared cache-with-fallback helper for data providers that fetch JSON directly
via requests (bypassing libs/soup.py's save_downloaded_soup(), typically because
they need a bespoke multi-step session, GraphQL pagination, or CSRF handshake -
see hu_benu.py, hu_ofotert.py, hu_shell.py, hu_rossmann.py, hu_tesco.py,
hu_alzabox.py, hu_dpd.py).

Each of those providers used to duplicate the same inline caching logic, which
had a real gap: a source under maintenance can return a syntactically valid but
empty/near-empty result (e.g. a degraded API responding {"stores": []} instead
of erroring out), and that empty result would silently overwrite a previously
good, non-empty cache - with nothing to fall back to on the next run either.
load_or_fetch_json() closes that gap: a fetch that raises, or comes back empty
while a previous non-empty cache exists, falls back to that previous cache
instead of losing it.
"""

try:
    import logging
    import sys
    import os
    import json
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def load_or_fetch_json(cache_file, download_cache, fetch_fn, use_cached_data):
    """Read `cache_file` as JSON if `use_cached_data` is True and it exists;
    otherwise call `fetch_fn()` for fresh data, falling back to whatever's
    already cached if the fetch raises or comes back empty.

    Args:
        cache_file (str): Local cache file path (JSON).
        download_cache (str): Directory `cache_file` lives in - created if missing.
        fetch_fn (callable): Zero-arg function returning fresh data (a list or
            dict) from the live source. May raise.
        use_cached_data (bool): config.get_download_use_cached_data() - if True
            and cache_file already exists, it's used as-is without calling
            fetch_fn() at all.

    Returns:
        The freshly fetched data, the previously cached data (if the fetch
        failed/was empty, or use_cached_data forced reuse), or None/empty if
        neither is available.
    """
    if use_cached_data is True and os.path.isfile(cache_file):
        with open(cache_file, mode='r', encoding='utf-8') as file:
            return json.load(file)

    previous = None
    if os.path.isfile(cache_file):
        try:
            with open(cache_file, mode='r', encoding='utf-8') as file:
                previous = json.load(file)
        except Exception as e:
            logging.warning('Cached copy of %s is unreadable (%s).', cache_file, e)

    try:
        data = fetch_fn()
    except Exception as e:
        logging.warning('Fetch failed (%s) - source likely down/under maintenance.', e)
        if previous:
            logging.warning('Falling back to the previously cached copy of %s.', cache_file)
            return previous
        return None

    if not data and previous:
        logging.warning('Fetch for %s returned no data (source likely down/under '
                        'maintenance) - falling back to the previously cached copy '
                        'instead of overwriting it with an empty result.', cache_file)
        return previous

    if not os.path.exists(download_cache):
        os.makedirs(download_cache)
    with open(cache_file, mode='w', encoding='utf-8') as file:
        json.dump(data, file)
    return data
