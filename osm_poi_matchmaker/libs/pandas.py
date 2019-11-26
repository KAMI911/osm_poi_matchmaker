# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from sys import exit
    import requests
    import os
    import pandas as pd
    from utils import config
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


def save_downloaded_pd(link, file, verify=config.get_download_verify_link()):
    if config.get_download_use_cached_data() == True and os.path.isfile(file):
        df = pd.read_csv(file)
    else:
        df = pd.read_csv(link, encoding='UTF-16', sep='\t')
        if df is not None:
            if not os.path.exists(config.get_directory_cache_url()):
                os.makedirs(config.get_directory_cache_url())
            df.to_csv(file)
        else:
            logging.warning('Skipping dataset.')
    return df
