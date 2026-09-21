# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

MAPS_LINK_COORDS_RE = re.compile(r'query=(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)')

FUEL_AVOID_NAME = '(mol|shell|m. petrol|avia|lukoil|hunoil|orlen|omv|oil)'
GROCERY_AVOID_NAME = '(aldi|cba|príma|prima|coop|lidl|penny|reál|real|spar|tesco|s-market|smarket|tom market|tommarket)'


class hu_auchan(DataProvider):
    """Imports Auchan supermarket and petrol station locations in Hungary. Petrol
    stations come from a plain JSON API (auchan.hu/api/v2/cache/petrol/list); stores
    come from a Nuxt.js SSR hydration payload embedded on auchan.hu/aruhazak (the
    site renders its store list client-side from this, with no separate JSON API) -
    __resolve_nuxt_payload() below reimplements the same reference-chasing logic
    Nuxt's own hydration code uses to expand it back into plain nested objects."""

    def contains(self):
        self.petrol_link = 'https://auchan.hu/api/v2/cache/petrol/list'
        self.stores_link = 'https://auchan.hu/aruhazak'
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/120.0 Safari/537.36'}
        self.tags = {'brand': 'Auchan', 'brand:wikidata': 'Q758603'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huauchsup = {'shop': 'supermarket'}
        huauchsup.update(self.tags)
        huauchsup.update(POS_HU_GEN)
        huauchsup.update(PAY_CASH)
        huauchfu = {'amenity': 'fuel'}
        huauchfu.update(self.tags)
        self.__types = [
            {'poi_code': 'huauchsup', 'poi_common_name': 'Auchan', 'poi_type': 'shop', 'poi_tags': huauchsup,
             'poi_url_base': 'https://auchan.hu', 'poi_search_name': 'auchan', 'additional_ref_name': 'ref',
             'poi_search_avoid_name': GROCERY_AVOID_NAME,
             'osm_search_distance_perfect': 1000, 'osm_search_distance_safe': 400},
            {'poi_code': 'huauchfu', 'poi_common_name': 'Auchan', 'poi_type': 'fuel', 'poi_tags': huauchfu,
             'poi_url_base': 'https://auchan.hu', 'poi_search_name': 'auchan', 'additional_ref_name': 'ref',
             'poi_search_avoid_name': FUEL_AVOID_NAME,
             'osm_search_distance_perfect': 500, 'osm_search_distance_safe': 100, 'osm_search_distance_unsafe': 20},
        ]
        return self.__types

    @staticmethod
    def __build_street(address):
        parts = [(address.get('streetName') or '').strip(), (address.get('streetType') or '').strip(),
                 (address.get('streetNo') or '').strip()]
        return ' '.join(p for p in parts if p) or None

    @staticmethod
    def __resolve_nuxt_payload(html):
        """Expand Nuxt's devalue-style hydration payload: a flat array where most
        entries are plain values, but some are ["ShallowReactive"|"Reactive"|"Ref", n]
        wrapper pairs or dict/list nodes whose own values are indices into the same
        array. Mirrors the equivalent JS resolver Nuxt's client runtime itself uses."""
        marker = 'id="__NUXT_DATA__">'
        start = html.find(marker)
        if start == -1:
            return None
        start += len(marker)
        end = html.find('</script>', start)
        if end == -1:
            return None
        payload = json.loads(html[start:end])

        def resolve(idx, seen=frozenset()):
            if not isinstance(idx, int) or idx < 0 or idx >= len(payload) or idx in seen:
                return None
            seen = seen | {idx}
            node = payload[idx]
            if isinstance(node, list):
                if len(node) == 2 and node[0] in ('ShallowReactive', 'Reactive', 'Ref'):
                    return resolve(node[1], seen)
                return [resolve(x, seen) if isinstance(x, int) else x for x in node]
            if isinstance(node, dict):
                return {k: (resolve(v, seen) if isinstance(v, int) else v) for k, v in node.items()}
            return node

        pinia = resolve(payload[1]['pinia'])
        return pinia['storesAndPetrolsStore']['stores']

    def __process_petrol(self):
        soup = save_downloaded_soup(self.petrol_link, os.path.join(self.download_cache, self.filename),
                                    self.filetype, False, headers=self.headers)
        if soup is None:
            return
        for poi_data in json.loads(soup):
            try:
                address = poi_data.get('address') or {}
                self.data.code = 'huauchfu'
                self.data.ref = clean_string(poi_data.get('id'))
                name = clean_string(poi_data.get('name')) or ''
                self.data.branch = name.removeprefix('MH Auchan ').removeprefix('Auchan ') \
                    .removesuffix(' benzinkút') or None
                self.data.city = clean_city(address.get('city'))
                self.data.postcode = clean_string(address.get('postCode'))
                street = self.__build_street(address)
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                    extract_street_housenumber_better_2(street)
                self.data.original = clean_string(street)
                coords = address.get('coordinates') or {}
                self.data.lat, self.data.lon = check_hu_boundary(
                    coords.get('latitude'), coords.get('longitude'))
                self.data.public_holiday_open = False
                self.data.add()
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
                logging.exception(poi_data)

    def __process_stores(self):
        # FileType.csv (not .html) deliberately - the __NUXT_DATA__ script tag holds a
        # compact JSON blob that BeautifulSoup's HTML prettifying would reformat/corrupt;
        # .csv/.json are the only FileTypes save_downloaded_soup() passes through as a
        # raw, unmodified string (see libs/soup.py's readfile()/download_content()).
        html = save_downloaded_soup(self.stores_link, os.path.join(self.download_cache, 'hu_auchan_stores.html'),
                                    FileType.csv, False, headers=self.headers)
        if html is None:
            return
        try:
            stores = self.__resolve_nuxt_payload(html)
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            logging.error('Could not resolve Auchan stores Nuxt payload: %s', e)
            return
        for poi_data in stores or []:
            try:
                address = poi_data.get('address') or {}
                self.data.code = 'huauchsup'
                self.data.ref = clean_string(poi_data.get('id'))
                name = clean_string(poi_data.get('name')) or ''
                self.data.branch = name.removeprefix('Auchan ') or None
                self.data.city = clean_city(address.get('city'))
                self.data.postcode = clean_string(address.get('postCode'))
                street = self.__build_street(address)
                self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                    extract_street_housenumber_better_2(street)
                self.data.original = clean_string(street)
                lat, lon = None, None
                maps_link = poi_data.get('googleMapsLink') or ''
                coords_match = MAPS_LINK_COORDS_RE.search(maps_link)
                if coords_match is not None:
                    lat, lon = coords_match.group(1), coords_match.group(2)
                self.data.lat, self.data.lon = check_hu_boundary(lat, lon)
                self.data.phone = clean_string(poi_data.get('phoneNumber'))
                self.data.public_holiday_open = False
                self.data.add()
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
                logging.exception(poi_data)

    def process(self):
        try:
            self.__process_petrol()
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
        try:
            self.__process_stores()
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
