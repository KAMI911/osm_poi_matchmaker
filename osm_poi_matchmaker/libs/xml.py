# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import requests
    import os
    from lxml import etree
    from utils import config
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


def download_xml(link, verify_link=config.get_download_verify_link()):
    try:
        page = requests.get(link, verify=verify_link)
    except requests.exceptions.ConnectionError as e:
        logging.warning('Unable to open connection.')
        return None
    return page.content if page.status_code == 200 else None


def save_downloaded_xml(link, file, verify=config.get_download_verify_link()):
    if config.get_download_use_cached_data() == True and os.path.isfile(file):
        with open(file, 'r', encoding='utf-8') as content_file:
            page = content_file.read()
    else:
        page = download_xml(link, verify)
        if page != None:
            if not os.path.exists(config.get_directory_cache_url()):
                os.makedirs(config.get_directory_cache_url())
            with open(file, mode='w', encoding='utf-8') as code:
                code.write(page.decode('utf-8'))
        else:
            logging.warning('Skipping dataset.')
    return page
