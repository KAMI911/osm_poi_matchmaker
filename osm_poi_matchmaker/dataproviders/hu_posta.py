# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    from lxml import etree
    import traceback
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_phone_to_str, clean_street, clean_street_type, \
        clean_string, clean_email
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.enums import WeekDaysLongHU
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)


class hu_posta(DataProvider):

    def contains(self):
        self.link = 'https://httpmegosztas.posta.hu/PartnerExtra/OUT/PostInfo.xml'
        self.tags = {'operator': 'Magyar Posta Zrt.',
                     'operator:addr': '1138 Budapest, Dunavirág utca 2-6.', 'ref:HU:vatin': '10901232-2-44',
                     'ref:vatin': 'HU10901232', 'brand:wikipedia': 'hu:Magyar Posta Zrt.', 'brand:wikidata': 'Q145614',
                     'contact:email': 'ugyfelszolgalat@posta.hu', 'contact:phone': '+36 1 767 8200',
                     'contact:facebook': 'https://www.facebook.com/MagyarPosta',
                     'contact:youtube': 'https://www.youtube.com/user/magyarpostaofficial',
                     'contact:instagram': 'https://www.instagram.com/magyar_posta_zrt', 'payment:cash': 'yes',
                     'payment:debit_cards': 'yes'}
        self.filetype = FileType.xml
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hupostapo = {'amenity': 'post_office', 'brand': 'Magyar Posta'}
        hupostapo.update(self.tags)
        hupostacse = {'amenity': 'vending_machine', 'vending': 'cheques', 'brand': 'Magyar Posta'}
        hupostacse.update(self.tags)
        hupostacso = {'amenity': 'parcel_locker', 'parcel_mail_in': 'yes', 'brand': 'MPL', 'parcel_pickup': 'yes',
                      'colour': 'green', 'material': 'metal', 'refrigerated': 'no'}
        hupostacso.update(self.tags)
        hupostapp = {'post_office': 'post_partner', 'post_office:brand': 'PostaPont',
                     'post_office:brand:wikidata': 'Q145614'}
        hupostamp = {'amenity': 'post_office', 'brand': 'Mobil Posta'}
        hupostamp.update(self.tags)
        self.__types = [
            {'poi_code': 'hupostapo', 'poi_common_name': 'Posta', 'poi_type': 'post_office',
             'poi_tags': hupostapo, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta', 'export_poi_name': False,
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 350, 'osm_search_distance_unsafe': 220,
             'preserve_original_post_code': True},
            {'poi_code': 'hupostacse', 'poi_common_name': 'Posta csekkbefizető automata',
             'poi_type': 'vending_machine_cheques',
             'poi_tags': hupostacse, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 220},
            {'poi_code': 'hupostacso', 'poi_common_name': 'Posta csomagautomata',
             'poi_type': 'vending_machine_parcel_locker',
             'poi_tags': hupostacso, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(mpl|posta)', 'export_poi_name': False,
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200, 'osm_search_distance_unsafe': 2},
            {'poi_code': 'hupostapp', 'poi_common_name': 'PostaPont', 'poi_type': 'post_partner',
             'poi_tags': hupostapp, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(postapont|posta)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300,
             'osm_search_distance_unsafe': 220, 'preserve_original_post_code': True},
            {'poi_code': 'hupostamp', 'poi_common_name': 'Mobilposta', 'poi_type': 'post_office', 'export_poi_name': False,
             'poi_tags': hupostamp, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            for poi_data in soup.findAll('post'):
                try:
                    # If this is a closed post office, skip it
                    # if poi_data.get('ispostpoint') == '0':
                    #    continue
                    #  The 'kirendeltség' post offices are not available to end users, so we remove them
                    servicepointtype = clean_string(poi_data.servicepointtype.get_text().upper()) if poi_data.servicepointtype is not None else None
                    if 'okmányiroda' in poi_data.find('name').get_text().lower() or \
                            'mol kirendeltség' in poi_data.find('name').get_text().lower():
                        logging.debug('Skipping non public post office.')
                        continue
                    else:
                        if servicepointtype == 'PM':
                            self.data.code = 'hupostapo'
                            self.data.public_holiday_open = False
                        elif servicepointtype == 'CS':
                            self.data.code = 'hupostacso'
                            self.data.public_holiday_open = True
                        elif servicepointtype == 'PP':
                            self.data.code = 'hupostapp'
                            self.data.public_holiday_open = False
                        else:
                            logging.error('Non existing Posta type: {}'.format(poi_data.servicepointtype.get_text().upper()))
                        self.data.postcode = clean_string(poi_data.get('zipcode'))
                        self.data.housenumber = poi_data.street.housenumber.get_text().split('(', 1)[0].strip() \
                            if poi_data.street.housenumber is not None else None
                        if self.data.housenumber == 'belterület HRSZ 3162':
                            self.data.housenumber = None
                            self.data.conscriptionnumber = '3162'
                        self.data.conscriptionnumber = None
                        self.data.city = clean_city(poi_data.city.get_text())
                        self.data.branch = clean_string(poi_data.find('name').get_text()
                        ) if poi_data.find('name') is not None else None
                        if self.data.code == 'hupostapo':
                            self.data.branch = re.sub(
                                r"(\d{1,3})", r"\1. számú", self.data.branch)
                        days = poi_data.findAll('days') if poi_data.findAll(
                            'days') is not None else None
                        nonstop_num = 0
                        for d in days:
                            if len(d) != 0:
                                day_key = None
                                # Try to match day name in data source (day tag) with on of WeekDaysLongHU enum element
                                # Select day based on d.day matching
                                for rd in WeekDaysLongHU:
                                    if d.day is not None:
                                        if rd.name == d.day.get_text():
                                            day_key = rd.value
                                            break
                                        else:
                                            day_key = None
                                    else:
                                        day_key = None
                                # No day matching skip to next
                                # Skip days that are not exist at data provider's
                                if day_key is None:
                                    logging.warning('Cannot find any opening hours information for day {}.'.
                                                    format(rd.name))
                                    continue
                                else:
                                    # Extract from and to information
                                    from1 = d.from1.get_text() if d.from1 is not None else None
                                    to1 = d.to1.get_text() if d.to1 is not None else None
                                    from2 = d.from2.get_text() if d.from2 is not None else None
                                    to2 = d.to2.get_text() if d.to2 is not None else None
                                    # Avoid duplicated values of opening and close
                                    if from1 != from2 and to1 != to2:
                                        logging.debug('Opening hours in post office: %s: %s-%s; %s-%s.',
                                                      self.data.branch, from1, to1, from2, to2)
                                        self.data.day_open(day_key, from1)
                                        if from2 is None or to2 is None:
                                            self.data.day_close(day_key, from1)
                                            # Count opening hours with nonstop like settings
                                            if from1 in '0:00' and to1 in ['0:00', '23:59', '24:00']:
                                                nonstop_num += 1
                                        else:
                                            # Check on Wednesday if there is a lunch break
                                            # Only same lunch break is supported for every days
                                            if day_key == 3:
                                                self.data.lunch_break_start = to1
                                                self.data.lunch_break_stop = from2
                                            self.data.day_close(day_key, to2)
                                            # Count opening hours with nonstop like settings
                                            if from1 in '0:00' and to2 in ['0:00', '23:59', '24:00']:
                                                nonstop_num += 1
                                    else:
                                        # It seems there are duplications in Posta data source
                                        # Remove duplicates
                                        logging.warning('Duplicated opening hours in post office: %s: %s-%s; %s-%s.',
                                                        self.data.branch, from1, to1, from2, to2)
                                        from2, to2 = None, None
                        # All times are open so it is non-stop
                        if nonstop_num >= 7:
                            logging.debug('It is a non stop post office.')
                            self.data.nonstop = True
                        self.data.lat, self.data.lon = \
                            check_hu_boundary(poi_data.gpsdata.wgslat.get_text().replace(',', '.'),
                                              poi_data.gpsdata.wgslon.get_text().replace(',', '.'))
                        # Get street name and type
                        street_tmp_1 = clean_street(poi_data.street.find('name').get_text().strip()) \
                            if poi_data.street.find('name') is not None else None
                        street_tmp_2 = clean_street_type(poi_data.street.find('type').get_text().strip()) \
                            if poi_data.street.find('type') is not None else None
                        # Streets without types
                        if street_tmp_2 is None:
                            self.data.street = street_tmp_1
                            # Since there is no original address format we create one
                            if self.data.housenumber is not None:
                                self.data.original = '{} {}'.format(
                                    street_tmp_1, self.data.housenumber)
                            else:
                                self.data.original = '{}'.format(street_tmp_1)
                        # Street with types
                        elif street_tmp_1 is not None and street_tmp_2 is not None:
                            self.data.street = '{} {}'.format(
                                street_tmp_1, street_tmp_2)
                            # Since there is no original address format we create one
                            if self.data.housenumber is not None:
                                self.data.original = '{} {} {}'.format(street_tmp_1, street_tmp_2,
                                                                       self.data.housenumber)
                            else:
                                self.data.original = '{} {}'.format(
                                    street_tmp_1, street_tmp_2)
                        else:
                            logging.error(
                                'Non handled state in street data processing!')
                        self.data.phone = clean_phone_to_str(poi_data.phonearea.get_text()) \
                            if poi_data.phonearea is not None else None
                        self.data.email = clean_email(poi_data.email.get_text()) if poi_data.email is not None else None
                        self.data.add()
                except Exception as err:
                    logging.exception('Exception occurred: {}'.format(err))
                    logging.exception(traceback.format_exc())
                    logging.exception(poi_data)
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(e))
            logging.exception(traceback.format_exc())
            logging.exception(soup)
