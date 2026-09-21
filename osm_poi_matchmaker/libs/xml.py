# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import requests
    import os
    from lxml import etree
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.download_guard import looks_like_html
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def download_xml(link, verify_link=config.get_download_verify_link()):
    """Download an XML document over HTTP.

    Args:
        link (str): URL to fetch.
        verify_link (bool): Whether to verify TLS certificates.

    Returns:
        bytes | None: Raw response body on HTTP 200, or None on a connection error
        or a non-200 status.
    """
    try:
        page = requests.get(link, verify=verify_link)
    except requests.exceptions.ConnectionError as e:
        logging.warning('Unable to open connection.')
        return None
    return page.content if page.status_code == 200 else None


def _is_usable_xml(page, link):
    """True if `page` (bytes) is well-formed XML, logging a specific reason
    otherwise. Catches both the observed real-world failure (an HTML
    "maintenance"/error page on an HTTP 200 - see download_guard.looks_like_html)
    and any other malformed/truncated response, so a bad response never gets
    treated as usable just because the HTTP request itself "succeeded"."""
    if looks_like_html(page):
        logging.warning('%s looks like an HTML page, not XML (source likely '
                        'down/under maintenance) - ignoring it.', link)
        return False
    try:
        etree.fromstring(page)
    except Exception as e:
        logging.warning('%s did not parse as XML (%s) - ignoring it.', link, e)
        return False
    return True


def save_downloaded_xml(link, file, verify=config.get_download_verify_link()):
    """Download an XML document and cache it to disk, or reuse the cached copy.

    Simpler, XML-only predecessor of libs/soup.py's save_downloaded_soup() (no ETag
    check - if download.use.cached.data is True and the file exists, it's used as-is
    regardless of freshness). Used by dataproviders/hu_generic.py for Magyar Posta's
    ZipCodes.xml and StreetTypes.xml.

    A source occasionally serves an HTML "under maintenance"/error page (or other
    malformed content) in place of the real XML, on an otherwise-200 response
    (seen live with Magyar Posta) - see _is_usable_xml(). Such a response is
    treated the same as a failed download: the previously cached copy is read and
    returned instead, and the cache file is left untouched (never overwritten
    with the bad page). If the download fails outright (connection error,
    non-200) the same fallback applies.

    Args:
        link (str): URL to fetch.
        file (str): Local cache file path.
        verify (bool): Whether to verify TLS certificates.

    Returns:
        bytes | None: The XML content (freshly downloaded, or a previously cached
        copy), or None if nothing usable is available either way.
    """
    if config.get_download_use_cached_data() is True and os.path.isfile(file):
        with open(file, 'rb') as content_file:
            page = content_file.read()
    else:
        page = download_xml(link, verify)
        if page is not None and not _is_usable_xml(page, link):
            page = None
        if page is not None:
            if not os.path.exists(config.get_directory_cache_url()):
                os.makedirs(config.get_directory_cache_url())
            with open(file, mode='wb') as code:
                code.write(page)
        elif os.path.isfile(file):
            logging.warning('Falling back to the previously cached copy of %s.', file)
            with open(file, 'rb') as content_file:
                page = content_file.read()
        else:
            logging.warning('Skipping dataset: %s. No usable download and no cached copy.', link)
    return page
