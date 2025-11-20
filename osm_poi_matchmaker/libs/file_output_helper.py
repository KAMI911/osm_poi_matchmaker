# -*- coding: utf-8 -*-

try:
    import logging
    import traceback
    import math
    from osm_poi_matchmaker.libs.address import clean_url
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')


def url_tag_generator(poi_url_base: str, poi_website: str) -> str:
    if poi_url_base is not None and isinstance(poi_url_base, str) and poi_url_base != '':
        if poi_website is not None and isinstance(poi_website, str) and poi_website != '':
            if poi_url_base in poi_website:
                # The POI website contains the base URL use the POI website field only
                return clean_url('{}'.format(poi_website))
            else:
                # The POI website does not contain the base URL, but POI website contains another URL use that
                if '://' in poi_website:
                    return clean_url('{}'.format(poi_website))
                else:
                    # The POI website does not contain the base URL use the merged base URL and POI website field
                    return clean_url('{}/{}'.format(poi_url_base, poi_website))
        else:
            return clean_url('{}'.format(poi_url_base))
    else:
        return None
