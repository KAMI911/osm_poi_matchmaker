# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import requests
    import os
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


def download_soup(link, verify_link=config.get_download_verify_link()):
    try:
        page = requests.get(link, verify=verify_link)
    except requests.exceptions.ConnectionError as e:
        logging.warning('Unable to open connection.')
        return None
    return BeautifulSoup(page.content, 'html.parser') if page.status_code == 200 else None


def save_downloaded_soup(link, file, verify=config.get_download_verify_link()):
    if config.get_download_use_cached_data() == True and os.path.isfile(file):
        with open(file, 'r') as content_file:
             soup = BeautifulSoup(content_file.read(), 'html.parser')
    else:
        soup = download_soup(link, verify)
    if soup != None:
        if not os.path.exists(config.get_directory_cache_url()):
            os.makedirs(config.get_directory_cache_url())
        with open(file, mode="w", encoding="utf8") as code:
            code.write(str(soup.prettify()))
    else:
        logging.warning('Skipping dataset.')
    return soup
