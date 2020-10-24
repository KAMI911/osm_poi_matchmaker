# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import re
    from lxml import etree
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import clean_city, clean_phone_to_str, clean_street, clean_street_type
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.enums import WeekDaysLongHU
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)


class hu_posta(DataProvider):

    def constains(self):
        self.link = 'http://httpmegosztas.posta.hu/PartnerExtra/OUT/PostInfo.xml'
        self.tags = {'brand': 'Magyar Posta', 'operator': 'Magyar Posta Zrt.',
                     'operator:addr': '1138 Budapest, Dunavirág utca 2-6.', 'ref:vatin:hu': '10901232-2-44',
                     'ref:vatin': 'HU10901232', 'brand:wikipedia': 'hu:Magyar Posta Zrt.', 'brand:wikidata': 'Q145614',
                     'contact:email': 'ugyfelszolgalat@posta.hu', 'phone': '+3617678200',
                     'contact:facebook': 'https://www.facebook.com/MagyarPosta',
                     'contact:youtube': 'https://www.youtube.com/user/magyarpostaofficial',
                     'contact:instagram': 'https://www.instagram.com/magyar_posta_zrt', 'payment:cash': 'yes',
                     'payment:debit_cards': 'yes'}
        self.filetype = FileType.xml
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hupostapo = {'amenity': 'post_office'}
        hupostapo.update(self.tags)
        hupostacse = {'amenity': 'vending_machine', 'vending': 'cheques'}
        hupostacse.update(self.tags)
        hupostacso = {'amenity': 'vending_machine', 'vending': 'parcel_pickup'}
        hupostacso.update(self.tags)
        hupostapp = {'amenity': 'post_office'}
        hupostapp.update(self.tags)
        hupostamp = {'amenity': 'post_office'}
        hupostamp.update(self.tags)
        self.__types = [
            {'poi_code': 'hupostapo', 'poi_name': 'Posta', 'poi_type': 'post_office',
             'poi_tags': hupostapo, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 350, 'osm_search_distance_unsafe': 220,
             'preserve_original_post_code': True},
            {'poi_code': 'hupostacse', 'poi_name': 'Posta csekkbefizető automata',
             'poi_type': 'vending_machine_cheques',
             'poi_tags': hupostacse, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300, 'osm_search_distance_unsafe': 220},
            {'poi_code': 'hupostacso', 'poi_name': 'Posta csomagautomata',
             'poi_type': 'vending_machine_parcel_pickup',
             'poi_tags': hupostacso, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(mpl|posta)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 200},
            {'poi_code': 'hupostapp', 'poi_name': 'PostaPont', 'poi_type': 'post_office',
             'poi_tags': hupostapp, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': '(postapont|posta)',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300,
             'osm_search_distance_unsafe': 220, 'preserve_original_post_code': True},
            {'poi_code': 'hupostamp', 'poi_name': 'Mobilposta', 'poi_type': 'post_office',
             'poi_tags': hupostamp, 'poi_url_base': 'https://www.posta.hu', 'poi_search_name': 'posta',
             'osm_search_distance_perfect': 2000, 'osm_search_distance_safe': 300}]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            for e in soup.findAll('post'):
                try:
                    # If this is a closed post office, skip it
                    # if e.get('ispostpoint') == '0':
                    #    continue
                    #  The 'kirendeltség' post offices are not available to end users, so we remove them
                    if 'okmányiroda' in e.find('name').get_text().lower() or \
                            'mol kirendeltség' in e.find('name').get_text().lower():
                        logging.debug('Skipping non public post office.')
                        continue
                    else:
                        if e.servicepointtype.get_text() == 'PM':
                            self.data.name = 'Posta'
                            self.data.code = 'hupostapo'
                            self.data.public_holiday_open = False
                        elif e.servicepointtype.get_text() == 'CS':
                            self.data.name = 'Posta csomagautomata'
                            self.data.code = 'hupostacso'
                            self.data.public_holiday_open = True
                        elif e.servicepointtype.get_text() == 'PP':
                            self.data.name = 'PostaPont'
                            self.data.code = 'hupostapp'
                            self.data.public_holiday_open = False
                        else:
                            logging.error('Non existing Posta type.')
                        self.data.postcode = e.get('zipcode')
                        self.data.housenumber = e.street.housenumber.get_text().split('(', 1)[0].strip() \
                            if e.street.housenumber is not None else None
                        self.data.conscriptionnumber = None
                        self.data.city = clean_city(e.city.get_text())
                        self.data.branch = e.find('name').get_text(
                        ) if e.find('name') is not None else None
                        if self.data.code == 'hupostapo':
                            self.data.branch = re.sub(
                                r"(\d{1,3})", r"\1. számú", self.data.branch)
                        days = e.findAll('days') if e.findAll(
                            'days') is not None else None
                        nonstop_num = 0
                        for d in days:
                            if len(d) != 0:
                                day_key = None
                                # Try to match day name in data source (day tag) with on of WeekDaysLongHU enum element
                                # Select day based on d.day matching
                                for rd in WeekDaysLongHU:
                                    if rd.name == d.day.get_text():
                                        day_key = rd.value
                                        break
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
                                        logging.warning('Dulicated opening hours in post office: %s: %s-%s; %s-%s.',
                                                        self.data.branch, from1, to1, from2, to2)
                                        from2, to2 = None, None
                        # All times are open so it is non stop
                        if nonstop_num >= 7:
                            logging.debug('It is a non stop post office.')
                            self.data.nonstop = True
                        self.data.lat, self.data.lon = \
                            check_hu_boundary(e.gpsdata.wgslat.get_text().replace(',', '.'),
                                              e.gpsdata.wgslon.get_text().replace(',', '.'))
                        # Get street name and type
                        street_tmp_1 = clean_street(e.street.find('name').get_text().strip()) \
                            if e.street.find('name') is not None else None
                        street_tmp_2 = clean_street_type(e.street.type.get_text().strip()) \
                            if e.street.type is not None else None
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
                        self.data.phone = clean_phone_to_str(e.phonearea.get_text()) \
                            if e.phonearea is not None else None
                        self.data.email = e.email.get_text().strip() if e.email is not None else None
                        self.data.add()
                except Exception as err:
                    logging.error(err)
                    logging.error(e)
                    logging.exception('Exception occurred')

        except Exception as err:
            logging.exception('Exception occurred')

            logging.error(err)
