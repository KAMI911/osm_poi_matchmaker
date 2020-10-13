# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import requests
    import os
    import pandas as pd
    from osm_poi_matchmaker.utils import config
    from osm_poi_matchmaker.libs.soup import download_content
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.error(traceback.print_exc())
    sys.exit(128)

def save_downloaded_pd(link, file, verify=config.get_download_verify_link(), headers=None):
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
                    logging.info('The %s link returned error code other than 200 but there is an already downloaded file. Try to open it.', link)
                    df = pd.read_csv(file, encoding='UTF-8', sep='\t', skiprows=0)
                else:
                    logging.warning('Skipping dataset: %s. There is not downloadable URL, nor already downbloaded file.', link)
        else:
            if os.path.exists(file):
                df = pd.read_csv(file, encoding='UTF-8', sep='\t', skiprows=0)
                logging.info('Using file only: %s. There is not downloadable URL only just the file. Do not forget to update file manually!', file)
            else:
                logging.warning('Cannot use download and file: %s. There is not downloadable URL, nor already downbloaded file.', file)
    return df
