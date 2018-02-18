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
    page = requests.get(link, verify=verify_link)
    return BeautifulSoup(page.content, 'html.parser') if page.status_code == 200 else None


def save_downloaded_soup(link, file, verify=config.get_download_verify_link()):
    soup = download_soup(link, verify)
    if not os.path.exists(config.get_directory_cache_url()):
        os.makedirs(config.get_directory_cache_url())
    with open(file, mode="w", encoding="utf8") as code:
        code.write(str(soup.prettify()))
    return soup
