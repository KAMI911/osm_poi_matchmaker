# -*- coding: utf-8 -*-
import http

try:
    import logging
    import sys
    import requests
    import os
    import shutil
    import traceback
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.utils.enums import FileType
    from osm_poi_matchmaker.utils.cache import get_cached, set_cached
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def download_content(link, verify_link=config.get_download_verify_link(), post_parm=None, headers=None,
                     encoding='utf-8', json_body=False):
    """Fetch a URL over HTTP, as a plain GET or a POST with form/JSON data.

    Also records the response's ETag (if any) via utils/cache.set_cached(), so a
    later is_downloaded() call can check whether the cached copy is still current.

    Args:
        link (str): URL to fetch.
        verify_link (bool): Whether to verify TLS certificates.
        post_parm (dict | None): POST body data. If None, a GET is issued instead.
            If given, sent as JSON (when json_body=True) or as
            application/x-www-form-urlencoded form data (default).
        headers (dict | None): Extra request headers; a matching Content-Type is
            added automatically for POST requests.
        encoding (str): Encoding applied to page.encoding before reading page.text.
        json_body (bool): If True (and post_parm is set), POST post_parm as a JSON
            body instead of form-encoded data.

    Returns:
        bytes | str | None: Raw bytes if the response Content-Type is
        'application/zip', otherwise page.text; None on a connection error, any
        other exception, or a non-2xx/redirection status code (informational,
        client-error and server-error status codes all return None).
    """
    try:
        if post_parm is None:
            logging.debug('Downloading without post parameters.')
            page = requests.get(link, verify=verify_link, headers=headers, timeout=500)
            logging.debug('Downloaded without post parameters.')
            page.encoding = encoding
        elif json_body is True:
            logging.debug('Downloading with a JSON post body.')
            headers_static = {"Content-Type": "application/json; charset=UTF-8"}
            if headers is not None:
                headers.update(headers_static)
            else:
                headers = headers_static
            page = requests.post(link, verify=verify_link, json=post_parm, headers=headers, timeout=500)
            logging.debug('Downloaded with a JSON post body.')
            page.encoding = encoding
        else:
            logging.debug('Downloading with post parameters.')
            headers_static = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
            if headers is not None:
                headers.update(headers_static)
            else:
                headers = headers_static
            page = requests.post(link, verify=verify_link, data=post_parm, headers=headers, timeout=500)
            logging.debug('Downloaded with post parameters.')
            page.encoding = encoding
    except requests.exceptions.ConnectionError as e:
        logging.warning('Unable to open connection. (%s)', e)
        return None
    except Exception as e:
        logging.exception('Exception occurred: {}'.format(e))
        logging.exception(traceback.format_exc())
        return None
    etag = page.headers.get('ETag')
    if etag is not None:
        set_cached('etag:{}'.format(link), etag)
    else:
        logging.debug("No ETag in response: link=%s", link)

    returned_status = http.HTTPStatus(page.status_code)

    if (
        returned_status.is_informational
        or returned_status.is_client_error
        or returned_status.is_server_error
    ):
        suggestion = (
            "Perhaps you have to add a User-Agent header to the request "
            "because the API operator bans HTTP requests having default Requests library headers?"
            if headers is None
            else "Please check if it works in your browser!"
        )
        logging.error(f"{link} returned a {page.status_code} status code. {suggestion}")
        return None
    elif returned_status.is_redirection:
        logging.warning(
            f"{link} returned a {page.status_code} redirection status code. Please update the URL if it has been moved permanently!"
        )

    if page.headers.get("Content-Type") == "application/zip":
        logging.debug("Returning a ZIP file content.")
        return page.content
    else:
        logging.debug("Returning text content.")
        return page.text


def is_downloaded(link: str, verify_link=config.get_download_verify_link(), headers=None) -> bool:
    """Check whether the locally cached copy of `link` (see download_content()'s
    ETag caching) is still current, via a lightweight HEAD request.

    Args:
        link (str): URL to check.
        verify_link (bool): Whether to verify TLS certificates.
        headers (dict | None): Extra request headers for the HEAD request.

    Returns:
        bool: True only if we have a cached ETag for this link AND the server's
        current ETag matches it. False if there's no cached ETag at all (can't tell,
        so save_downloaded_soup() treats this as "needs downloading").
    """
    cache_key = 'etag:{}'.format(link)
    etag = get_cached(cache_key)
    if etag is not None:
        # fetch etag header to validate local file version
        response = requests.head(link, verify=verify_link, headers=headers)
        return etag == response.headers.get('ETag')
    return False


def save_downloaded_soup(link, file, filetype, skip_download=False, post_data=None, verify=config.get_download_verify_link(),
                         headers=None, json_body=False):
    """The main entry point data providers use to fetch and cache a source file, then
    get it back parsed (BeautifulSoup for html/xml, raw string for csv/json).

    Caching behaviour:
      - If skip_download=True, or download.use.cached.data is True and `file`
        already exists, the existing file is used without touching `link` at all
        (except zip files, which are just confirmed present - see extract_zip()).
      - Otherwise, if `file` exists and is_downloaded() confirms its ETag still
        matches, the existing file is reused.
      - Otherwise, `link` is fetched via download_content() and written to `file`
        (as pretty-printed HTML/XML, or raw text for csv/json). If the fetch fails
        but `file` already exists, that stale copy is used as a fallback.
      - If `link` is None, only the local `file` is read (useful for a
        manually/externally populated cache with no live source).

    Args:
        link (str | None): Source URL, or None to use `file` only.
        file (str): Local cache file path to read from/write to.
        filetype (FileType): Controls both how the response is parsed and how
            download_content() should treat it internally.
        skip_download (bool): Force-skip fetching; only meaningful for FileType.zip.
        post_data (dict | None): POST body passed through to download_content().
        verify (bool): Whether to verify TLS certificates.
        headers (dict | None): Extra request headers passed through to
            download_content().
        json_body (bool): Passed through to download_content() - POST post_data as
            JSON instead of form-encoded data.

    Returns:
        BeautifulSoup | str | bool | None: Parsed content for html/xml, raw string
        for csv/json, True for zip (once extracted/confirmed present), or None if
        nothing could be fetched or read.
    """
    logging.debug('save_downloaded_soup link={} file={} filetype={}'.format(link, file, filetype))

    if skip_download is True or (config.get_download_use_cached_data() is True and os.path.isfile(file)):
        # return true as a success marker, skip reading zip file to variable
        if filetype == FileType.zip:
            return True
    else:
        if link is not None:
            if os.path.exists(file) and is_downloaded(link, verify_link=verify, headers=headers):
                if filetype == FileType.zip:
                    return True
            else:
                content = download_content(link, verify, post_data, headers, json_body=json_body)
                if content is not None:
                    logging.info('We got content, write to file.')
                    if not os.path.exists(config.get_directory_cache_url()):
                        os.makedirs(config.get_directory_cache_url())
                    if filetype == FileType.zip:
                        with open(file, mode='wb') as file:
                            file.write(content)
                            return True
                    else:
                        with open(file, mode='w', encoding='utf-8') as code:
                            if filetype == FileType.html:
                                soup = BeautifulSoup(content, 'html.parser')
                                code.write(str(soup.prettify()))
                            elif filetype == FileType.xml:
                                soup = BeautifulSoup(content, 'lxml', from_encoding='utf-8')
                                logging.debug('original encoding: %s', soup.original_encoding)
                                code.write(str(soup.prettify()))
                            elif filetype == FileType.csv or filetype == FileType.json:
                                code.write(str(content))
                            else:
                                logging.error('Unexpected type to write: %s', filetype)
                else:
                    if os.path.exists(file):
                        logging.info(
                            'The %s link returned error code other than 200 but there is an already downloaded file. Try to open it.',
                            link)
                    else:
                        logging.warning(
                            'Skipping dataset: %s. There is not downloadable URL, nor already downloaded file.', link)
                        return None
        else:
            logging.info('Using file only: %s. There is not downloadable URL only just the file. Do not forget to update file manually!',
                file)
    if filetype == FileType.zip:
        return True
    if os.path.exists(file):
        content = readfile(file, filetype)
    else:
        logging.warning(
            'Cannot use download and file: %s. There is not downloadable URL, nor already downloaded file.', file)
        content = None
    return content


def readfile(r_filename: str, r_filetype: FileType):
    """Read a previously cached file and parse it according to its FileType.

    Args:
        r_filename (str): Path to the cached file.
        r_filetype (FileType): html/xml get parsed into a BeautifulSoup object;
            csv/json are returned as a raw string; other types log an error and
            return None (soup stays undefined).

    Returns:
        BeautifulSoup | str | None: Parsed content, or None if the file doesn't
        exist or reading/parsing raised.
    """
    try:
        if os.path.exists(r_filename):
            with open(r_filename, mode='r', encoding='utf-8') as code:
                if r_filetype == FileType.html:
                    soup = BeautifulSoup(code.read(), 'html.parser')
                elif r_filetype == FileType.xml:
                    soup = BeautifulSoup(code.read(), 'lxml')
                elif r_filetype == FileType.csv or r_filetype == FileType.json:
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
    """Unpack a downloaded zip archive to a directory.

    Args:
        filename (str): Path to the .zip file (or other shutil.unpack_archive-
            supported archive).
        dst_dir (str): Directory to extract into.
    """
    logging.debug('extract_zip filename={} to directory={}'.format(filename, dst_dir))
    if os.path.exists(filename):
        shutil.unpack_archive(filename, dst_dir)
    else:
        logging.error('extract_zip file={} does not exists'.format(filename))
