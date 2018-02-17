# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import requests
    from bs4 import BeautifulSoup
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)


def download_soup(link):
    page = requests.get(link, verify=False)
    return BeautifulSoup(page.content, 'html.parser') if page.status_code == 200 else None


def save_downloaded_soup(link, file):
    soup = download_soup(link)
    with open(file, mode="w", encoding="utf8") as code:
        code.write(str(soup.prettify()))
    return soup
