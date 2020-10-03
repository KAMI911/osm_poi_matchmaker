# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import requests
    import os
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


def download_content(link, verify_link=config.get_download_verify_link(), post_parm=None, headers=None, encoding='utf-8'):
    try:
        if post_parm is None:
            logging.debug('Downloading without post parameters.')
            page = requests.get(link, verify=verify_link, headers=headers)
            page.encoding = encoding
        else:
            logging.debug('Downloading with post parameters.')
            headers_static = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
            if headers is not None:
                headers.update(headers_static)
            else:
                headers = headers_static
            page = requests.post(link, verify=verify_link, data=post_parm, headers=headers)
            page.encoding = encoding
    except requests.exceptions.ConnectionError as e:
        logging.warning('Unable to open connection. ({})'.format(e))
        return None
    return page.text if page.status_code == 200 else None


def save_downloaded_soup(link, file, filetype, post_data=None, verify=config.get_download_verify_link(), headers=None):
    if config.get_download_use_cached_data() is True and os.path.isfile(file):
        soup = readfile(file, filetype)
    else:
        if link is not None:
            soup = download_content(link, verify, post_data, headers)
            if soup is not None:
                logging.info('We got content, write to file.')
                if not os.path.exists(config.get_directory_cache_url()):
                    os.makedirs(config.get_directory_cache_url())
                with open(file, mode='w', encoding='utf-8') as code:
                    if filetype == FileType.html:
                        soup = BeautifulSoup(soup, 'html.parser')
                        code.write(str(soup.prettify()))
                    elif filetype == FileType.xml:
                        soup = BeautifulSoup(soup, 'lxml', from_encoding='utf-8')
                        logging.debug('original encoding: {}'.format(soup.original_encoding))
                        code.write(str(soup.prettify()))
                    elif filetype == FileType.csv or filetype == FileType.json:
                        code.write(str(soup))
                    else:
                        logging.error('Unexpected type to write: {}'.format(filetype))
            else:
                if os.path.exists(file):
                    logging.info('The {} link returned error code other than 200 but there is an already downloaded file. Try to open it.'.format(link))
                    soup = readfile(file, filetype)
                else:
                    logging.warning('Skipping dataset: {}. There is not downloadable URL, nor already downbloaded file.'.format(link))
        else:
            if os.path.exists(file):
                soup = readfile(file, filetype)
                if filetype == FileType.html:
                    soup = BeautifulSoup(soup, 'html.parser')
                elif filetype == FileType.xml:
                    soup = BeautifulSoup(soup, 'lxml')
                logging.info('Using file only: {}. There is not downloadable URL only just the file. Do not forget to update file manually!'.format(file))
            else:
                logging.warning('Cannot use download and file: {}. There is not downloadable URL, nor already downbloaded file.'.format(file))
    return soup


def readfile(r_filename, r_filetype):
    try:
        if os.path.exists(r_filename):
            with open(r_filename, mode='r', encoding='utf-8') as code:
                if r_filetype == FileType.html:
                    soup = BeautifulSoup(code.read(), 'html.parser')
                elif r_filetype == FileType.csv or r_filetype == FileType.json or r_filetype == FileType.xml:
                    soup = code.read()
                else:
                    logging.error('Unexpected type to read: {}'.format(r_filetype))
            return soup
        else:
            return None
    except Exception as e:
        logging.error(e)
        logging.error(traceback.print_exc())
