# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import hashlib
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, \
        clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

WEEKDAY_INDEX = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
JSON_LD_RE = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.DOTALL)


class hu_amplifon(DataProvider):
    """Imports Amplifon hearing-aid clinic locations in Hungary. amplifon.com's HU
    sitemap (sitemap_store.xml) lists every store page URL; each page embeds a
    schema.org MedicalBusiness JSON-LD block with full structured address/hours/geo -
    the same data the page itself renders from, just directly parseable instead of
    needing DOM scraping."""

    def contains(self):
        self.link = 'https://www.amplifon.com/hu/sitemap_store.xml'
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'}
        self.tags = {'shop': 'hearing_aids', 'brand': 'Amplifon', 'brand:wikidata': 'Q477222',
                     'contact:website': 'https://www.amplifon.com/hu'}
        # FileType.csv for both the sitemap and each store page - raw passthrough,
        # parsed directly with regex below (same rationale as hu_sinsay.py/hu_auchan.py).
        self.filetype = FileType.csv
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huamplif = self.tags.copy()
        self.__types = [
            {'poi_code': 'huamplif', 'poi_common_name': 'Amplifon', 'poi_type': 'hearing_aids',
             'poi_tags': huamplif, 'poi_url_base': 'https://www.amplifon.com/hu', 'poi_search_name': 'amplifon',
             'additional_ref_name': 'ref', 'osm_search_distance_perfect': 500,
             'osm_search_distance_safe': 100, 'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    def __fetch_store_urls(self):
        raw = save_downloaded_soup(self.link, os.path.join(self.download_cache, self.filename),
                                   self.filetype, False, headers=self.headers)
        if raw is None:
            return []
        return re.findall(r'<loc>([^<]+)</loc>', raw)

    def __fetch_store_data(self, url):
        cache_name = 'hu_amplifon_store_{}.html'.format(hashlib.sha1(url.encode('utf-8')).hexdigest()[:16])
        html = save_downloaded_soup(url, os.path.join(self.download_cache, cache_name), self.filetype, False,
                                    headers=self.headers)
        if html is None:
            return None
        for block in JSON_LD_RE.findall(html):
            try:
                data = json.loads(block)
            except (json.JSONDecodeError, ValueError):
                continue
            # A JSON-LD block is sometimes a single object, sometimes a list of
            # them (schema.org allows both) - confirmed live: most Amplifon store
            # pages use the list form, which crashed every single store's import
            # with AttributeError ('list' object has no attribute 'get').
            candidates = data if isinstance(data, list) else [data]
            for candidate in candidates:
                # A city/region listing page carries a bare MedicalBusiness with only a text address (no
                # geo, no branch code); only a store page has the structured PostalAddress object.
                if isinstance(candidate, dict) and candidate.get('@type') == 'MedicalBusiness' \
                        and isinstance(candidate.get('address'), dict):
                    return candidate
        return None

    def process(self):
        try:
            store_urls = self.__fetch_store_urls()
            for url in store_urls:
                try:
                    store = self.__fetch_store_data(url)
                    if store is None:
                        continue
                    address = store.get('address') or {}
                    if (address.get('addressCountry') or '').upper() != 'HU':
                        continue
                    self.data.code = 'huamplif'
                    self.data.ref = clean_string(store.get('branchCode'))
                    name = clean_string(store.get('name')) or ''
                    self.data.branch = name.removeprefix('Amplifon').strip() or None
                    self.data.city = clean_city(address.get('addressLocality'))
                    self.data.postcode = clean_string(address.get('postalCode'))
                    self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                        extract_street_housenumber_better_2(address.get('streetAddress'))
                    self.data.original = clean_string(address.get('streetAddress'))
                    self.data.phone = clean_phone_to_str(store.get('telephone'))
                    geo = store.get('geo') or {}
                    self.data.lat, self.data.lon = check_hu_boundary(geo.get('latitude'), geo.get('longitude'))
                    for rule in store.get('openingHoursSpecification') or []:
                        day_index = WEEKDAY_INDEX.get(rule.get('dayOfWeek'))
                        if day_index is not None and rule.get('opens') and rule.get('closes'):
                            self.data.day_open_close(day_index, rule.get('opens'), rule.get('closes'))
                    self.data.public_holiday_open = False
                    self.data.add()
                except Exception as e:
                    logging.exception('Exception occurred: {}'.format(e))
                    logging.exception(traceback.format_exc())
                    logging.exception(url)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
