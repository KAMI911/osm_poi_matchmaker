# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import requests
    import os
    from bs4 import BeautifulSoup
    from utils import config
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


def download_soup(link, verify_link=config.get_download_verify_link(), post_parm=None, headers=None):
    try:
        if post_parm is None:
            logging.debug('Downloading without post parameters.')
            page = requests.get(link, verify=verify_link, headers=headers)
        else:
            logging.debug('Downloading with post parameters.')
            headers_static = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
            if headers is not None:
                headers = headers.items() + headers_static.items()
            else:
                headers = headers_static
            page = requests.post(link, verify=verify_link, data=post_parm, headers=headers)
    except requests.exceptions.ConnectionError as e:
        logging.warning('Unable to open connection. ({})'.format(e))
        return None
    return BeautifulSoup(page.content, 'html.parser') if page.status_code == 200 else None


def save_downloaded_soup(link, file, post_data=None, verify=config.get_download_verify_link(), headers=None):
    if config.get_download_use_cached_data() == True and os.path.isfile(file):
        with open(file, 'r', encoding='utf-8') as content_file:
            soup = BeautifulSoup(content_file.read(), 'html.parser')
    else:
        if link != None:
            soup = download_soup(link, verify, post_data, headers)
            if soup != None:
                if not os.path.exists(config.get_directory_cache_url()):
                    os.makedirs(config.get_directory_cache_url())
                with open(file, mode='w', encoding='utf-8') as code:
                    code.write(soup.get_text())
            else:
                if os.path.exists(file):
                    logging.info('The {} link returned error code other than 200 but there is an already downloaded file. Try to open it.'.format(link))
                    with open(file, mode='r', encoding='utf-8') as code:
                        soup = BeautifulSoup(code.read(), 'html.parser')
                else:
                    logging.warning('Skipping dataset: {}. There is not downloadable URL, nor already downbloaded file.'.format(link))
        else:
            if os.path.exists(file):
                with open(file, mode='r', encoding='utf-8') as code:
                    soup = BeautifulSoup(code.read(), 'html.parser')
                logging.info('Using file only: {}. There is not downloadable URL only just the file. Do not forget to update file manually!'.format(file))
            else:
                logging.warning('Cannot use download and file: {}. There is not downloadable URL, nor already downbloaded file.'.format(file))
    return soup
