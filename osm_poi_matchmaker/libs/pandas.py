# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import requests
    import os
    import pandas as pd
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.soup import download_content
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def save_downloaded_pd(link, file, verify=config.get_download_verify_link(), headers=None):
    """Download a tab-separated (UTF-16-encoded source, saved as UTF-8) dataset and
    load it as a pandas DataFrame, or read it from the local cache file if present.

    Note: the cached-file branch (used when download.use.cached.data is True) reads
    with pd.read_csv()'s defaults (comma-separated), while the other branches read
    with sep='\\t' - if the cache is ever hit for a genuinely tab-separated file, it
    will parse incorrectly.

    Args:
        link (str): URL of the tab-separated source file.
        file (str): Local cache file path.
        verify (bool): Whether to verify TLS certificates.
        headers: Extra HTTP headers to send (passed through, currently unused
            because they're never forwarded to download_content() below).

    Returns:
        pd.DataFrame: Parsed data. May be unbound (NameError on use) if neither a
        download nor an existing cache file was available - callers should not rely
        on this without checking the source exists.
    """
    if config.get_download_use_cached_data() is True and os.path.isfile(file):
        df = pd.read_csv(file)
    else:
        if link is not None:
            cvs = download_content(link, verify, None, None, 'utf-16')
            if cvs is not None:
                logging.info('We got content, write to file.')
                if not os.path.exists(config.get_directory_cache_url()):
                    os.makedirs(config.get_directory_cache_url())
                with open(file, mode='w', encoding='utf-8') as code:
                    code.write(cvs)
                df = pd.read_csv(file, encoding='UTF-8', sep='\t', skiprows=0)
            else:
                if os.path.exists(file):
                    logging.info(
                        'The %s link returned error code other than 200 but there is an already downloaded file. Try to open it.',
                        link)
                    df = pd.read_csv(file, encoding='UTF-8', sep='\t', skiprows=0)
                else:
                    logging.warning(
                        'Skipping dataset: %s. There is not downloadable URL, nor already downbloaded file.', link)
        else:
            if os.path.exists(file):
                df = pd.read_csv(file, encoding='UTF-8', sep='\t', skiprows=0)
                logging.info(
                    'Using file only: %s. There is not downloadable URL only just the file. Do not forget to update file manually!',
                    file)
            else:
                logging.warning(
                    'Cannot use download and file: %s. There is not downloadable URL, nor already downbloaded file.',
                    file)
    return df
