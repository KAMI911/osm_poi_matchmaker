# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import os
    import json
    from osm_poi_matchmaker.libs.soup import save_downloaded_soup
    from osm_poi_matchmaker.libs.address import extract_street_housenumber_better_2, clean_city, clean_string, \
        clean_url
    from osm_poi_matchmaker.libs.osm_tag_sets import POS_HU_GEN, PAY_CASH
    from osm_poi_matchmaker.libs.geo import check_hu_boundary
    from osm_poi_matchmaker.utils.data_provider import DataProvider
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class hu_aldi(DataProvider):
    """Imports Aldi supermarket locations in Hungary from the Uberall store-finder API."""

    def contains(self):
        # The old storefront endpoint (www.aldi.hu/.../.get-stores-in-radius.json) is gone. The site
        # now runs its store locator through Uberall (locations-as-a-service); this feed is richer than
        # ALDI's own Spryker "Glue" commerce API (asl.api.aldi.hu) - it also carries per-day opening
        # hours, phone and a store landing page URL that the Glue API doesn't expose at all.
        self.link = 'https://locator.uberall.com/api/storefinders/rDCKKjdtbi2w0Qx3Cq1axERNtccFqZ/locations/all' \
                   '?v=20260101&language=hu&fieldMask=id&fieldMask=lat&fieldMask=lng&fieldMask=country' \
                   '&fieldMask=city&fieldMask=streetAndNumber&fieldMask=zip&fieldMask=phone' \
                   '&fieldMask=openingHours&fieldMask=website'
        self.tags = {'operator': 'ALDI Magyarország Élelmiszer Bt.', 'operator:wikipedia': 'hu:Aldi_(Magyarország)',
                     'operator:addr': '2051 Biatorbágy, Mészárosok útja 2.', 'brand': 'Aldi',
                     'ref:HU:vatin': '22234663-2-44', 'ref:vatin': 'HU22234663',
                     'ref:HU:company': '13-06-058506',
                     'brand:wikipedia': 'hu:Aldi', 'brand:wikidata': 'Q41171672', 'operator:wikidata': 'Q61299364',
                     'contact:facebook': 'ALDI.Magyarorszag',
                     'contact:youtube': 'https://www.youtube.com/user/ALDIMagyarorszag',
                     'contact:instagram': 'aldi.magyarorszag',
                     'air_conditioning': 'yes', }
        self.tags.update(POS_HU_GEN)
        self.tags.update(PAY_CASH)
        self.filetype = FileType.json
        self.filename = '{}.{}'.format(
            self.__class__.__name__, self.filetype.name)

    def types(self):
        hualdisup = {'shop': 'supermarket',
                     'diet:gluten_free': 'yes',
                     'diet:lactose_free': 'yes',
                     'diet:vegan': 'yes',
                     'diet:url': 'https://szorolap.aldi.hu/mentes_laktozmentes/'}
        hualdisup.update(self.tags)
        self.__types = [
            {'poi_code': 'hualdisup', 'poi_common_name': 'Aldi', 'poi_type': 'shop', 'poi_tags': hualdisup,
             'poi_url_base': 'https://www.aldi.hu', 'poi_search_name': 'aldi',
             'osm_search_distance_perfect': 1000, 'osm_search_distance_safe': 400},
        ]
        return self.__types

    def process(self):
        try:
            soup = save_downloaded_soup('{}'.format(self.link), os.path.join(self.download_cache, self.filename),
                                        self.filetype)
            poi_dataset = []
            if soup is not None:
                # parse the html using beautiful soap and store in variable `soup`
                text = json.loads(soup, strict=False)
                for poi_data in (text.get('response') or {}).get('locations') or []:
                    try:
                        if poi_data.get('country') == 'HU':
                            self.data.code = 'hualdisup'
                            # Assign: code, postcode, city, name, branch, website, original
                            #         street, housenumber, conscriptionnumber, ref, geom
                            self.data.city = clean_city(poi_data.get('city'))
                            self.data.lat, self.data.lon = check_hu_boundary(poi_data.get('lat'),
                                                                             poi_data.get('lng'))
                            self.data.street, self.data.housenumber, self.data.conscriptionnumber = \
                                extract_street_housenumber_better_2(poi_data.get('streetAndNumber'))
                            self.data.postcode = clean_string(poi_data.get('zip'))
                            self.data.original = clean_string(poi_data.get('streetAndNumber'))
                            self.data.public_holiday_open = False
                            self.data.phone = clean_string(poi_data.get('phone'))
                            self.data.website = clean_url(poi_data.get('website'))
                            for opening_day in poi_data.get('openingHours') or []:
                                day_of_week = opening_day.get('dayOfWeek')
                                if day_of_week is not None:
                                    self.data.day_open_close(day_of_week - 1, opening_day.get('from1'),
                                                             opening_day.get('to1'))
                            self.data.add()
                    except Exception as e:
                        logging.error(e)
                        logging.error(poi_data)
                        logging.exception('Exception occurred')
        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')
