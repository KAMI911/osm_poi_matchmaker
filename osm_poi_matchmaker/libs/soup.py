# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import requests
    import os
    import shutil
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.utils.enums import FileType
    from osm_poi_matchmaker.utils.cache import get_cached, set_cached
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def download_content(link, verify_link=config.get_download_verify_link(), post_parm=None, headers=None,
                     encoding='utf-8'):
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
        logging.warning('Unable to open connection. (%s)', e)
        return None

    etag = page.headers.get('ETag')
    if etag is not None:
        set_cached('etag:{}'.format(link), etag)
    else:
        logging.warning('cant save etag value of response: link={} headers={}'.format(link, page.headers))

    if page.headers.get('Content-Type') == 'application/zip':
        return page.content if page.status_code == 200 else None
    else:
        return page.text if page.status_code == 200 else None


def is_downloaded(link: str, verify_link=config.get_download_verify_link(), headers=None) -> bool:
    cache_key = 'etag:{}'.format(link)
    etag = get_cached(cache_key)
    if etag is not None:
        # fetch etag header to validate local file version
        response = requests.head(link, verify=verify_link, headers=headers)
        return etag == response.headers.get('ETag')
    return False


def save_downloaded_soup(link, file, filetype, post_data=None, verify=config.get_download_verify_link(), headers=None):
    logging.debug('save_downloaded_soup link={} file={} filetype={}'.format(link, file, filetype))

    if config.get_download_use_cached_data() is True and os.path.isfile(file):
        # return true as a success marker, skip reading zip file to variable
        if filetype == FileType.zip:
            return True
        soup = readfile(file, filetype)
    else:
        if link is not None:
            if os.path.exists(file) and is_downloaded(link, verify_link=verify, headers=headers):
                return True if filetype == FileType.zip else readfile(file, filetype)

            soup = download_content(link, verify, post_data, headers)
            if soup is not None:
                logging.info('We got content, write to file.')

                if not os.path.exists(config.get_directory_cache_url()):
                    os.makedirs(config.get_directory_cache_url())

                if filetype == FileType.zip:
                    with open(file, mode='wb') as file:
                        file.write(soup)
                else:
                    with open(file, mode='w', encoding='utf-8') as code:
                        if filetype == FileType.html:
                            soup = BeautifulSoup(soup, 'html.parser')
                            code.write(str(soup.prettify()))
                        elif filetype == FileType.xml:
                            soup = BeautifulSoup(soup, 'lxml', from_encoding='utf-8')
                            logging.debug('original encoding: %s', soup.original_encoding)
                            code.write(str(soup.prettify()))
                        elif filetype == FileType.csv or filetype == FileType.json:
                            code.write(str(soup))
                        else:
                            logging.error('Unexpected type to write: %s', filetype)
            else:
                if os.path.exists(file):
                    logging.info(
                        'The %s link returned error code other than 200 but there is an already downloaded file. Try to open it.',
                        link)
                    soup = readfile(file, filetype)
                else:
                    logging.warning(
                        'Skipping dataset: %s. There is not downloadable URL, nor already downbloaded file.', link)
        else:
            if os.path.exists(file):
                soup = readfile(file, filetype)
                if filetype == FileType.html:
                    soup = BeautifulSoup(soup, 'html.parser')
                elif filetype == FileType.xml:
                    soup = BeautifulSoup(soup, 'lxml')
                logging.info(
                    'Using file only: %s. There is not downloadable URL only just the file. Do not forget to update file manually!',
                    file)
            else:
                logging.warning(
                    'Cannot use download and file: %s. There is not downloadable URL, nor already downbloaded file.',
                    file)
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
                    logging.error('Unexpected type to read: %s', r_filetype)
            return soup
        else:
            return None
    except Exception as e:
        logging.error(e)
        logging.exception('Exception occurred')


def extract_zip(filename: str, dst_dir: str):
    logging.debug('extract_zip filename={} to directory={}'.format(filename, dst_dir))
    if os.path.exists(filename):
        shutil.unpack_archive(filename, dst_dir)
    else:
        logging.error('extract_zip file={} does not exists'.format(filename))
