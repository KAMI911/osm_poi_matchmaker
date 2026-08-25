# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import requests
    import os
    from lxml import etree
    from osm_poi_matchmaker.utils import config
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


def save_downloaded_xml(link, file, verify=config.get_download_verify_link()):
    """Download an XML document and cache it to disk, or reuse the cached copy.

    Simpler, XML-only predecessor of libs/soup.py's save_downloaded_soup() (no ETag
    check - if download.use.cached.data is True and the file exists, it's used as-is
    regardless of freshness). Used by dataproviders/hu_generic.py for Magyar Posta's
    ZipCodes.xml and StreetTypes.xml.

    Args:
        link (str): URL to fetch.
        file (str): Local cache file path.
        verify (bool): Whether to verify TLS certificates.

    Returns:
        str | bytes | None: The XML content (str if read from cache, bytes if freshly
        downloaded), or None if the download failed and no cache file exists.
    """
    if config.get_download_use_cached_data() is True and os.path.isfile(file):
        with open(file, 'r', encoding='utf-8') as content_file:
            page = content_file.read()
    else:
        page = download_xml(link, verify)
        if page is not None:
            if not os.path.exists(config.get_directory_cache_url()):
                os.makedirs(config.get_directory_cache_url())
            with open(file, mode='w', encoding='utf-8') as code:
                code.write(page.decode('utf-8'))
        else:
            logging.warning('Skipping dataset.')
    return page
