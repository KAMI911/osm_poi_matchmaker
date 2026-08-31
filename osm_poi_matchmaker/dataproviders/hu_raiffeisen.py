# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    import json
    import traceback
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup, download_content
    from osm_poi_matchmaker.libs.address import extract_all_address_waxeye, clean_phone_to_str, clean_string
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType, WeekDaysLongHU
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


# Liferay portlet resources used by the branch/ATM search widget on the bankfiokok page.
RAIFFEISEN_PORTLET_URL = 'https://www.raiffeisen.hu/kapcsolat/bankfiokok'
RAIFFEISEN_PORTLET_NS = '_raiffeisenfiokkeresodisplay_WAR_raiffeisenfiokkeresoportlet_'
RAIFFEISEN_ADDRESSES_URL = (
    RAIFFEISEN_PORTLET_URL +
    '?p_p_id=raiffeisenfiokkeresodisplay_WAR_raiffeisenfiokkeresoportlet'
    '&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=cmdGetAddresses'
    '&p_p_cacheability=cacheLevelPage'
    '&' + RAIFFEISEN_PORTLET_NS + 'categoryTypeId={}'
)
RAIFFEISEN_INFO_WINDOW_URL = (
    RAIFFEISEN_PORTLET_URL +
    '?p_p_id=raiffeisenfiokkeresodisplay_WAR_raiffeisenfiokkeresoportlet'
    '&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=cmdGetInfoWindowContent'
    '&p_p_cacheability=cacheLevelPage'
)
RAIFFEISEN_BRANCH_DETAIL_URL = 'https://www.raiffeisen.hu/kapcsolat/bankfiokok/-/adatlap/fiok/{}'
# Budapest addresses are given as "Budapest <roman numeral>. kerület, <street> <no>", which the
# shared Hungarian address parser doesn't recognize; strip the district clause before parsing.
RAIFFEISEN_KERULET_RE = re.compile(r'\s+[IVXLCDM]+\.\s*ker[üu]let,?', re.IGNORECASE)


class hu_raiffeisen(DataProvider):
    """Imports Raiffeisen Bank branch and ATM locations in Hungary from Raiffeisen's Liferay portlet backend."""

    def contains(self):
        self.link = RAIFFEISEN_ADDRESSES_URL.format('BRANCH')
        self.tags = {'brand': 'Raiffeisen Bank', 'operator': 'Raiffeisen Bank Zrt.',
                     'operator:addr': '1054 Budapest, Akadémia utca 6.',
                     'ref:HU:vatin': '10801316-4-44', 'ref:vatin': 'HU10801316',
                     'contact:website': 'https://www.raiffeisen.hu',
                     'air_conditioning': 'yes'}
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(self.__class__.__name__, self.filetype.name)

    def types(self):
        huraiffbank = {'amenity': 'bank'}
        huraiffbank.update(self.tags)
        huraiffatm = {'amenity': 'atm'}
        huraiffatm.update(self.tags)
        self.__types = [
            {'poi_code': 'huraiffbank', 'poi_common_name': 'Raiffeisen Bank', 'poi_type': 'bank',
             'poi_tags': huraiffbank, 'poi_url_base': 'https://www.raiffeisen.hu', 'poi_search_name': 'raiffeisen',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 400, 'osm_search_distance_safe': 100,
             'osm_search_distance_unsafe': 40},
            {'poi_code': 'huraiffatm', 'poi_common_name': 'Raiffeisen Bank ATM', 'poi_type': 'atm',
             'poi_tags': huraiffatm, 'poi_url_base': 'https://www.raiffeisen.hu',
             'poi_search_name': '(raiffeisen atm|raiffeisen)',
             'additional_ref_name': 'ref',
             'osm_search_distance_perfect': 100, 'osm_search_distance_safe': 30,
             'osm_search_distance_unsafe': 10},
        ]
        return self.__types

    def _set_address(self, address_text):
        address_for_parsing = RAIFFEISEN_KERULET_RE.sub('', address_text, count=1)
        self.data.postcode, self.data.city, self.data.street, self.data.housenumber, \
            self.data.conscriptionnumber = extract_all_address_waxeye(address_for_parsing)
        original = clean_string(address_text)
        if original and len(original) > 512:
            logging.warning('Address field exceeds 512 char limit, truncating: %s', original[:100])
            self.data.original = original[:512]
        else:
            self.data.original = original

    def process_branches(self):
        # The list endpoint only gives coordinates and an opaque identifier per branch
        # (e.g. 'branch_268_47.78_19.92'); name, address, phone and opening hours only
        # exist on each branch's own detail page, so one extra request per branch is
        # unavoidable here.
        link = RAIFFEISEN_ADDRESSES_URL.format('BRANCH')
        soup = save_downloaded_soup(link, os.path.join(self.download_cache, self.filename), self.filetype)
        if soup is None:
            return
        addresses = json.loads(soup).get('addresses') or []
        logging.info('Found %d Raiffeisen branch locations.', len(addresses))
        for entry in addresses:
            try:
                identifier = entry.get('identifier') or ''
                parts = identifier.split('_')
                if len(parts) < 2 or parts[0] != 'branch':
                    continue
                branch_id = parts[1]
                detail_link = RAIFFEISEN_BRANCH_DETAIL_URL.format(branch_id)
                detail_file = os.path.join(self.download_cache,
                                           'hu_raiffeisen_branch_{}.html'.format(branch_id))
                detail_soup = save_downloaded_soup(detail_link, detail_file, FileType.html)
                if detail_soup is None:
                    continue
                address_span = detail_soup.find('span', class_='branch-address')
                if address_span is None:
                    logging.warning('No address found for branch %s.', branch_id)
                    continue

                self.data.code = 'huraiffbank'
                self._set_address(address_span.get_text().strip())
                self.data.lat, self.data.lon = check_hu_boundary(entry.get('lat'), entry.get('lng'))
                self.data.ref = clean_string(branch_id)
                self.data.public_holiday_open = False

                phone_label = detail_soup.find('td', class_='contacts-list-type',
                                               string=re.compile('Telefonszám'))
                if phone_label is not None:
                    phone_value = phone_label.find_next_sibling('td', class_='contacts-list-value')
                    if phone_value is not None:
                        self.data.phone = clean_phone_to_str(phone_value.get_text().strip())

                # Opening hours table: one row per weekday, separate columns for the
                # branch floor and the cashdesk - we only care about the branch hours.
                for row in detail_soup.select('table.branch-opening-table tbody tr'):
                    day_cell = row.find('td', class_='opening')
                    hours_cell = row.find('td', class_='branch')
                    if day_cell is None or hours_cell is None:
                        continue
                    day_name = day_cell.get_text().strip()
                    day_index = None
                    for wd in WeekDaysLongHU:
                        if wd.name == day_name:
                            day_index = wd.value
                            break
                    if day_index is None:
                        continue
                    hours_text = hours_cell.get_text().strip()
                    if hours_text.lower() == 'zárva':
                        continue
                    hour_parts = [p.strip() for p in hours_text.split('-')]
                    if len(hour_parts) == 2:
                        self.data.day_open(day_index, hour_parts[0])
                        self.data.day_close(day_index, hour_parts[1])

                self.data.add()
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
                logging.exception(entry)

    def process_atms(self):
        # ATM detail pages (adatlap/atm/<id>) are empty shells with no server-rendered content
        # (unlike branches), so the address has to come from the same info-window AJAX call the
        # map popup uses; there is no phone or opening hours data available for ATMs at all.
        link = RAIFFEISEN_ADDRESSES_URL.format('ATM')
        content = download_content(link)
        if content is None:
            return
        addresses = json.loads(content).get('addresses') or []
        logging.info('Found %d Raiffeisen ATM locations.', len(addresses))
        for entry in addresses:
            try:
                identifier = entry.get('identifier') or ''
                parts = identifier.split('_')
                if len(parts) < 2 or parts[0] != 'atm':
                    continue
                atm_id = parts[1]
                info_content = download_content(RAIFFEISEN_INFO_WINDOW_URL, post_parm={
                    RAIFFEISEN_PORTLET_NS + 'identifier': identifier,
                    RAIFFEISEN_PORTLET_NS + 'categoryTypeId': 'ATM',
                })
                if info_content is None:
                    continue
                address_text = (json.loads(info_content).get('windowContentData') or {}).get('address')
                if not address_text:
                    logging.warning('No address found for ATM %s.', atm_id)
                    continue

                self.data.code = 'huraiffatm'
                self._set_address(address_text.strip())
                self.data.lat, self.data.lon = check_hu_boundary(entry.get('lat'), entry.get('lng'))
                self.data.ref = clean_string(atm_id)
                self.data.nonstop = True
                self.data.add()
            except Exception as e:
                logging.exception('Exception occurred: {}'.format(e))
                logging.exception(traceback.format_exc())
                logging.exception(entry)

    def process(self):
        try:
            self.process_branches()
            self.process_atms()
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
