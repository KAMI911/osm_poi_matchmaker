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


def download_soup(link, verify_link=config.get_download_verify_link(), post_parm=None):
    try:
        if post_parm is None:
            logging.debug('Downloading without post parameters.')
            page = requests.get(link, verify=verify_link)
        else:
            logging.debug('Downloading with post parameters.')
            headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
            page = requests.post(link, verify=verify_link, data=post_parm, headers=headers)
    except requests.exceptions.ConnectionError as e:
        logging.warning('Unable to open connection. ({})'.format(e))
        return None
    return BeautifulSoup(page.content, 'html.parser') if page.status_code == 200 else None


def save_downloaded_soup(link, file, post_data=None, verify=config.get_download_verify_link()):
    if config.get_download_use_cached_data() == True and os.path.isfile(file):
        with open(file, 'r') as content_file:
            soup = BeautifulSoup(content_file.read(), 'html.parser')
    else:
        soup = download_soup(link, verify, post_data)
        if soup != None:
            if not os.path.exists(config.get_directory_cache_url()):
                os.makedirs(config.get_directory_cache_url())
            with open(file, mode="w", encoding="utf8") as code:
                code.write(soup.get_text())
        else:
            logging.warning('Skipping dataset: {}.'.format(link))
    return soup
