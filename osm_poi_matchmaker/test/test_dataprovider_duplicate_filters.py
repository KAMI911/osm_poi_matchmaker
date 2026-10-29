# -*- coding: utf-8 -*-

"""Regression tests for providers that used to create duplicate or wrongly typed POIs
(found by comparing the "new" output of a full run with the "existing" one):

* hu_dpd.py listed Foxpost A-BOX / AlzaBox lockers ("DPD FOX ...", "DPD AlzaBox ...") that
  hu_foxpost.py / hu_alzabox.py already import as the very same physical lockers.
* hu_shell.py exported Shell Recharge only sites ("EV TESCO ...") as amenity=fuel stations.
* hu_mobiliti_ev.py emitted one POI per charge point, so a single site became several nodes.
"""

try:
    import json
    import logging
    import sys
    import unittest
    from unittest.mock import MagicMock, patch
    from osm_poi_matchmaker.dataproviders import hu_dpd, hu_shell
    from osm_poi_matchmaker.dataproviders.hu_mobiliti_ev import hu_mobiliti_ev
    from osm_poi_matchmaker.dataproviders import hu_mol_bubi
    import pandas as pd
    from osm_poi_matchmaker.libs.address import is_non_street_address, strip_branch_prefix, tidy_branch_case
    from osm_poi_matchmaker.dataproviders import hu_oil, hu_posta
    from osm_poi_matchmaker.libs.file_output import keep_osm_ref_spelling, generate_geojson
    from osm_poi_matchmaker.libs.poi_dataset import POIDatasetRaw
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class RecordingData:
    """Stand-in for POIDatasetRaw: remembers a snapshot of the set fields on every add()."""

    def __init__(self):
        self.rows = []

    def add(self):
        self.rows.append({k: v for k, v in vars(self).items() if k != 'rows'})

    def __getattr__(self, name):
        # day_open(), day_open_close(), ... and anything else the provider only calls
        return MagicMock()


def run_process(module, provider_class, stations, loader='load_or_fetch_json'):
    # POIDatasetRaw() opens a database connection in its constructor
    with patch('osm_poi_matchmaker.utils.data_provider.POIDatasetRaw', RecordingData):
        provider = provider_class(None, '/nonexistent')
    with patch.object(module, loader, return_value=stations):
        provider.process()
    return provider.data.rows


def dpd_shop(shop_id, name, pudo_type):
    return {'id': shop_id, 'name': name, 'pudoType': pudo_type, 'position': {'lat': 47.5, 'lon': 19.0},
            'address': {'streetName': 'Fő utca', 'houseNumber': '1', 'zipCode': '1011', 'city': 'Budapest',
                        'country': 'HU'},
            'businessHours': {}}


class TestDpdPartnerLockers(unittest.TestCase):
    def test_foxpost_and_alzabox_lockers_are_skipped(self):
        rows = run_process(hu_dpd, hu_dpd.hu_dpd, [
            dpd_shop('a', 'DPD FOX Bp. 03. ker. Govill', 400),
            dpd_shop('b', 'DPD AlzaBox (Príma)', 400),
            dpd_shop('c', 'DPD GLSBox Coop Tarcal', 400),
        ])
        self.assertEqual(['c'], [r['ref'] for r in rows])
        self.assertEqual('hudpdcso', rows[0]['code'])

    def test_staffed_pickup_point_with_fox_in_name_is_kept(self):
        rows = run_process(hu_dpd, hu_dpd.hu_dpd, [dpd_shop('d', 'Fox Bt. csomagpont', 100)])
        self.assertEqual(['d'], [r['ref'] for r in rows])
        self.assertEqual('hudpdpp', rows[0]['code'])


def shell_station(station_id, name, site_category):
    return {'id': station_id, 'name': name, 'site_category': site_category, 'country_code': 'HU',
            'lat': 47.5, 'lng': 19.0, 'address': 'FO UTCA 1.', 'city': 'BUDAPEST', 'postcode': '1011',
            'telephone': '', 'amenities': []}


class TestShellEvOnlySites(unittest.TestCase):
    def test_ev_only_sites_are_skipped(self):
        rows = run_process(hu_shell, hu_shell.hu_shell, [
            shell_station('1', 'EV TESCO KAPOSVAR', 'destination_charging_ev'),
            shell_station('2', 'ALKOTÁS utca', 'conventional_fuel_site'),
            shell_station('3', 'GYŐR', 'conventional_fuel_site_with_ev'),
        ])
        self.assertEqual(['Alkotás utca', 'Győr'], [r['branch'] for r in rows])


def mobiliti_location(loc_id, lat, lon, evses, operator='MVM Mobiliti Kft.', postal_code='1024'):
    return {'id': loc_id, 'latitude': str(lat), 'longitude': str(lon), 'postalCode': postal_code,
            'operator': {'name': operator}, 'evses': [{'evseId': e} for e in evses]}


class TestMobilitiSiteMerge(unittest.TestCase):
    def test_nearby_charge_points_become_one_site(self):
        locations = [
            mobiliti_location('B', 47.50010, 19.0, ['b1']),
            mobiliti_location('A', 47.50000, 19.0, ['a1', 'a2']),
            mobiliti_location('C', 47.50020, 19.0, ['c1']),  # 22 m from B, 44 m from A: chained
        ]
        merged = hu_mobiliti_ev.merge_nearby_sites(locations)
        self.assertEqual(1, len(merged))
        self.assertEqual('A', merged[0]['id'])
        self.assertEqual(['a1', 'a2', 'b1', 'c1'], sorted(e['evseId'] for e in merged[0]['evses']))

    def test_different_operator_postcode_or_distance_stay_separate(self):
        locations = [
            mobiliti_location('A', 47.5000, 19.0, ['a']),
            mobiliti_location('B', 47.5000, 19.0, ['b'], operator='TEA MOBILITÁS Kft.'),
            mobiliti_location('C', 47.5000, 19.0, ['c'], postal_code='1025'),
            mobiliti_location('D', 47.5010, 19.0, ['d']),  # 110 m away
        ]
        self.assertEqual(['A', 'B', 'C', 'D'], sorted(m['id'] for m in hu_mobiliti_ev.merge_nearby_sites(locations)))

    def test_input_is_not_mutated_and_no_evse_is_lost(self):
        locations = [mobiliti_location('A', 47.5, 19.0, ['a']), mobiliti_location('B', 47.5, 19.0, ['b'])]
        merged = hu_mobiliti_ev.merge_nearby_sites(locations)
        self.assertEqual(['a'], [e['evseId'] for e in locations[0]['evses']])
        self.assertEqual(2, len(merged[0]['evses']))

    def test_location_without_coordinates_is_passed_through(self):
        broken = {'id': 'X', 'latitude': None, 'longitude': None, 'evses': []}
        merged = hu_mobiliti_ev.merge_nearby_sites([broken, mobiliti_location('A', 47.5, 19.0, ['a'])])
        self.assertEqual(['A', 'X'], sorted(m['id'] for m in merged))


class TestRefHandling(unittest.TestCase):
    """hu_mol_bubi used to write the station address ('Ménesi út 10') as ref over OSM's station numbers, and the
    ref tag used to be rewritten even if the OSM value was the same apart from letter case."""

    def test_bubi_does_not_export_title_as_ref(self):
        station = {'id': '9399', 'name': '110139', 'title': 'Ménesi út 10', 'state': 'active', 'enabled': True,
                   'coord': {'lat': 47.4797, 'lng': 19.0479}}
        rows = run_process(hu_mol_bubi, hu_mol_bubi.hu_mol_bubi, json.dumps({'data': {'dockGroups': [station]}}),
                           loader='save_downloaded_soup')
        self.assertEqual(1, len(rows))
        self.assertNotIn('ref', rows[0])

    def test_osm_ref_kept_when_only_case_differs(self):
        self.assertEqual('HU53', keep_osm_ref_spelling('hu53', 'HU53'))

    def test_osm_ref_list_kept_when_it_contains_the_provider_ref(self):
        self.assertEqual('HUMOL0058;HUMOL0158', keep_osm_ref_spelling('HUMOL0158', 'HUMOL0058;HUMOL0158'))

    def test_provider_ref_used_when_different_or_missing(self):
        self.assertEqual('0218', keep_osm_ref_spelling('0218', '0230'))
        self.assertEqual('hu53', keep_osm_ref_spelling('hu53', None))
        self.assertEqual('hu53', keep_osm_ref_spelling('hu53', ''))


class TestNonStreetAddress(unittest.TestCase):
    """Motorways, numbered main roads and rest areas are not streets: they go to addr:place, not addr:street."""

    def test_non_street_values(self):
        for value in ('M7 autópálya', 'M0 Annahegyi pihenő', 'Velencei pihenőhely', '4. számú főközlekedési út',
                      '8 sz. számú főközlekedési út', '4-es főút', '2/a főút', 'Külterület 3 Számú főút',
                      'Pihenő M1 autópálya', 'M7-s aupá. Fvári pihenő'):
            self.assertTrue(is_non_street_address(value), value)

    def test_real_streets_stay_streets(self):
        # 'Bolyki főút' and 'Pihenő utca' are genuine street names
        for value in ('Bolyki főút', 'Pihenő utca', 'Fő utca', 'M. Petrol utca', 'Kossuth Lajos utca', '', None):
            self.assertFalse(is_non_street_address(value), value)

    def test_geojson_exports_addr_place(self):
        df = pd.DataFrame([
            {'poi_lon': 19.0, 'poi_lat': 47.5, 'poi_addr_place': 'M7 autópálya', 'poi_addr_housenumber': '167'},
            {'poi_lon': 19.1, 'poi_lat': 47.6, 'poi_addr_street': 'Fő utca', 'poi_addr_housenumber': '1'},
        ])
        features = json.loads(generate_geojson(df))['features']
        self.assertEqual({'addr:place': 'M7 autópálya', 'addr:housenumber': '167'}, features[0]['properties'])
        self.assertEqual({'addr:street': 'Fő utca', 'addr:housenumber': '1'}, features[1]['properties'])

    def test_process_street_moves_non_street_to_place_without_road_lookup(self):
        with patch('osm_poi_matchmaker.libs.poi_dataset.POIBase') as poi_base:
            data = POIDatasetRaw()
            data.street = 'M0 Annahegyi pihenő'
            data.process_street()
            self.assertEqual('M0 Annahegyi pihenő', data.addr_place)
            self.assertIsNone(data.street)
            poi_base.return_value.query_name_road_around.assert_not_called()

    def test_added_row_matches_the_column_list(self):
        # one place in add()'s row tuple and POI_COLS_RAW going out of step would silently shift every column
        with patch('osm_poi_matchmaker.libs.poi_dataset.POIBase'):
            data = POIDatasetRaw()
            data.code, data.lat, data.lon, data.city, data.postcode = 'x', 47.5, 19.0, 'Budapest', '1011'
            data.street, data.housenumber = 'M7 autópálya', '167'
            data.add()
            row = data.process().iloc[0]
        self.assertEqual('M7 autópálya', row['poi_addr_place'])
        self.assertIsNone(row['poi_addr_street'])
        self.assertEqual('167', row['poi_addr_housenumber'])

    def test_clear_all_resets_addr_place(self):
        with patch('osm_poi_matchmaker.libs.poi_dataset.POIBase'):
            data = POIDatasetRaw()
            data.street = 'M7 autópálya'
            data.process_street()
            data.clear_all()
            self.assertIsNone(data.addr_place)


class TestBranch(unittest.TestCase):
    """branch should only tell the location apart: no repeated brand, internal numbers or ALL CAPS."""

    def test_strip_branch_prefix(self):
        self.assertEqual('Balkány X40 Shop', strip_branch_prefix('FOXPOST A-BOX Balkány X40 Shop', 'FOXPOST A-BOX'))
        self.assertEqual('Carol Art Cafe', strip_branch_prefix('Packeta Z-Pont - Carol Art Cafe', 'Packeta Z-Pont'))
        self.assertEqual('Zalaegerszeg Belváros',
                         strip_branch_prefix('BENU Gyógyszertár Zalaegerszeg Belváros', 'BENU Gyógyszertár'))
        self.assertEqual('Veszprém', strip_branch_prefix('Veszprém', 'BENU Gyógyszertár'))
        self.assertIsNone(strip_branch_prefix('FOXPOST A-BOX', 'FOXPOST A-BOX'))
        self.assertIsNone(strip_branch_prefix(None, 'FOXPOST A-BOX'))

    def test_tidy_branch_case(self):
        self.assertEqual('Hédervár', tidy_branch_case('HÉDERVÁR'))
        self.assertEqual('Kiskunfél II', tidy_branch_case('KISKUNFÉL II'))
        self.assertEqual('Budapest Corvin', tidy_branch_case('Budapest Corvin'))
        self.assertIsNone(tidy_branch_case(None))

    def test_oil_branch_is_the_town_not_a_quote_character(self):
        store = {'store': 'OIL! MAGLÓD', 'lat': '47.44', 'lng': '19.35', 'address': 'Liszt Ferenc utca 6/A.',
                 'city': 'Maglód', 'zip': '2234', 'phone': '', 'id': '908', 'url': ''}
        rows = run_process(hu_oil, hu_oil.hu_oil, json.dumps([store]), loader='save_downloaded_soup')
        self.assertEqual(['Maglód'], [r['branch'] for r in rows])

    def test_mobiliti_station_number_is_the_ref(self):
        location = mobiliti_location('HU_MMV_1', 47.5, 19.0, [])
        location.update({'name': 'Oroszlány, Uszoda, 14006 - Mobiliti', 'city': 'Oroszlány', 'address': 'Fő utca 1.'})
        foreign = mobiliti_location('HU_AUD_2', 47.6, 19.1, [], operator='Audi', postal_code='9027')
        foreign.update({'name': 'Győr, Audi Hungária út - Audi Hungária Zrt.', 'city': 'Győr', 'address': 'Audi út 1'})
        rows = run_process(sys.modules[hu_mobiliti_ev.__module__], hu_mobiliti_ev,
                           json.dumps([location, foreign]), loader='save_downloaded_soup')
        by_code = {r['code']: r for r in rows}
        self.assertEqual('14006', by_code['humobilchs']['ref'])
        self.assertEqual('Oroszlány, Uszoda', by_code['humobilchs']['branch'])
        # another operator's site keeps its own name and gets no Mobiliti station ref
        self.assertEqual('Audi', by_code['humobothchs']['name'])
        self.assertIsNone(by_code['humobothchs'].get('ref'))
        self.assertEqual('Győr, Audi Hungária út - Audi Hungária Zrt.', by_code['humobothchs']['branch'])


    def test_posta_automata_number_is_the_ref(self):
        from bs4 import BeautifulSoup
        xml = '''<postinfo><post ispostpoint="1" zipcode="1826"><id>107852</id>
            <name>277 sz. automata - XIX. Citroen u Mobil Petrol</name><city>Budapest</city>
            <street><name>Citroen</name><type>utca</type><housenumber>2</housenumber></street>
            <gpsdata><wgslat>47,438068</wgslat><wgslon>19,145779</wgslon></gpsdata>
            <servicepointtype>CS</servicepointtype></post></postinfo>'''
        rows = run_process(hu_posta, hu_posta.hu_posta, BeautifulSoup(xml, 'lxml'), loader='save_downloaded_soup')
        self.assertEqual('hupostacso', rows[0]['code'])
        self.assertEqual('277', rows[0]['ref'])
        self.assertEqual('XIX. Citroen u Mobil Petrol', rows[0]['branch'])


class TestPoiTagsHoldOnlyOsmTags(unittest.TestCase):
    """hu_tesco.py's hutescoexp once had 'additional_ref_name': 'ref' (a poi_common column, already set one level up)
    inside its poi_tags dict, so it was written to the OSM nodes as a real tag."""

    INTERNAL_KEYS = {'additional_ref_name', 'poi_code', 'poi_common_name', 'poi_type', 'poi_search_name',
                     'poi_search_avoid_name', 'osm_search_distance_perfect', 'osm_search_distance_safe',
                     'osm_search_distance_unsafe', 'poi_url_base', 'preserve_original_name'}

    def test_no_provider_puts_internal_keys_into_poi_tags(self):
        import importlib
        import pkgutil
        import osm_poi_matchmaker.dataproviders as package
        from osm_poi_matchmaker.utils.data_provider import DataProvider
        bad = {}
        checked = 0
        with patch('osm_poi_matchmaker.utils.data_provider.POIDatasetRaw', RecordingData):
            for info in pkgutil.iter_modules(package.__path__):
                # hu_helyi_kozlekedes builds its types from downloaded GTFS feeds (a minute of retries offline)
                if not info.name.startswith('hu_') or info.name == 'hu_helyi_kozlekedes':
                    continue
                try:
                    module = importlib.import_module('osm_poi_matchmaker.dataproviders.' + info.name)
                    classes = [c for c in vars(module).values()
                               if isinstance(c, type) and issubclass(c, DataProvider) and c is not DataProvider
                               and c.__module__ == module.__name__]
                    for cls in classes:
                        for poi_type in cls(None, '/nonexistent').types():
                            checked += 1
                            leaked = self.INTERNAL_KEYS & set((poi_type.get('poi_tags') or {}).keys())
                            if leaked:
                                bad[(info.name, poi_type.get('poi_code'))] = sorted(leaked)
                except Exception:
                    continue  # a provider that can't be built offline is outside this check
        self.assertGreater(checked, 50)
        self.assertEqual({}, bad)


if __name__ == '__main__':
    unittest.main()


class TestExpandStreetU(unittest.TestCase):
    def test_abbreviation_expanded(self):
        from osm_poi_matchmaker.libs.address import expand_street_u
        self.assertEqual('Béke utca', expand_street_u('Béke u.'))
        self.assertEqual('ZRÍNYI utca', expand_street_u('ZRÍNYI u'))
        self.assertEqual('Jókai M. utca', expand_street_u('Jókai M.u.'))
        self.assertEqual('Lugos utca, 5', expand_street_u('Lugos u, 5'))

    def test_other_streets_unchanged(self):
        from osm_poi_matchmaker.libs.address import expand_street_u
        for street in ('Fő út', 'Tulipán köz', 'Kossuth utca', None, ''):
            self.assertEqual(street, expand_street_u(street))


class TestEvSockets(unittest.TestCase):
    def test_mixed_values_listed_highest_first_and_counts_summed(self):
        from osm_poi_matchmaker.dataproviders.hu_mobiliti_ev import summarize_evses
        evses = [
            {'evseId': 'A', 'plugType': 'Type2', 'cableAttached': False, 'quantity': 1,
             'power': 11000, 'current': 16, 'voltage': 230},
            {'evseId': 'B', 'plugType': 'type2', 'cableAttached': False, 'quantity': 1,
             'power': 22000, 'current': 32, 'voltage': 230},
            {'evseId': 'C', 'plugType': 'CCS', 'cableAttached': True, 'quantity': 2,
             'power': 150000, 'current': 300, 'voltage': 500},
            {'evseId': 'C', 'plugType': 'CCS', 'cableAttached': True, 'quantity': 2,
             'power': 150000, 'current': 300, 'voltage': 500},
            {'plugType': 'schuko', 'cableAttached': False, 'quantity': 1, 'power': 3600, 'current': 16,
             'voltage': 230},
        ]
        fields = summarize_evses(evses)
        self.assertEqual(2, fields['socket_type2_cableless'])
        self.assertEqual('22 kW;11 kW', fields['socket_type2_cableless_output'])
        self.assertEqual('32;16', fields['socket_type2_cableless_current'])
        self.assertEqual('230', fields['socket_type2_cableless_voltage'])
        self.assertEqual(2, fields['socket_type2_combo'])
        self.assertEqual('150 kW', fields['socket_type2_combo_output'])
        self.assertEqual(1, fields['socket_schuko'])
        self.assertEqual('3.6 kW', fields['socket_schuko_output'])
        self.assertNotIn('socket_schuko_current', fields)
        self.assertEqual(5, fields['capacity'])

    def test_tesla_and_cee_sockets(self):
        from osm_poi_matchmaker.dataproviders.hu_mobiliti_ev import summarize_evses
        fields = summarize_evses([
            {'plugType': 'tesla', 'currentType': 'DC', 'quantity': 1, 'power': 250000, 'current': 500, 'voltage': 500},
            {'plugType': 'tesla', 'currentType': 'DC', 'quantity': 1, 'power': 150000, 'current': 350, 'voltage': 480},
            {'plugType': 'Tesla', 'currentType': 'AC', 'quantity': 1, 'power': 11000, 'current': 16, 'voltage': 400},
            {'plugType': 'iec60309_5', 'currentType': 'AC', 'quantity': 1, 'power': 11000, 'current': 16,
             'voltage': 400},
            {'plugType': 'iec60309_5', 'currentType': 'AC', 'quantity': 1, 'power': 22000, 'current': 32,
             'voltage': 400},
        ])
        self.assertEqual(2, fields['socket_type2_combo'])
        self.assertEqual('250 kW;150 kW', fields['socket_type2_combo_output'])
        self.assertEqual(1, fields['socket_type2_cable'])
        self.assertEqual('11 kW', fields['socket_type2_cable_output'])
        self.assertEqual(1, fields['socket_cee_red_16a'])
        self.assertEqual('22 kW', fields['socket_cee_red_32a_output'])

    def test_fractional_kw_not_truncated(self):
        from osm_poi_matchmaker.dataproviders.hu_mobiliti_ev import format_kw
        self.assertEqual('7.4 kW', format_kw(7400))
        self.assertEqual('3.7 kW', format_kw(3700))
        self.assertEqual('22 kW', format_kw(22000))

    def test_cableless_current_column_name(self):
        from osm_poi_matchmaker.dao import poi_array_structure as s
        for cols in (s.POI_COLS, s.POI_DB, s.POI_DB_RAW):
            self.assertFalse([c for c in cols if 'curent' in c])


class TestMobilitiOperators(unittest.TestCase):
    def test_classification(self):
        from osm_poi_matchmaker.dataproviders.hu_mobiliti_ev import classify_operator
        self.assertEqual(('humobilchs', None), classify_operator(None))
        self.assertEqual(('humobilchs', None), classify_operator('MVM Mobiliti Kft.'))
        self.assertEqual(('humobilchs', None), classify_operator('MVM MOBILITI KFT.'))
        self.assertEqual(('hueonchs', 'E.ON'), classify_operator('E.ON'))
        self.assertEqual(('hulidlchs', 'Lidl'), classify_operator('LIDL Magyarország Bt.'))
        self.assertEqual(('huteachs', 'TEA'), classify_operator('HU*TEA'))
        self.assertEqual(('hutslachs', 'Tesla Supercharger'), classify_operator('Tesla'))
        self.assertEqual(('humobothchs', 'Hungexpo'), classify_operator('Hungexpo'))

    def test_operators_with_their_own_import_are_skipped(self):
        from osm_poi_matchmaker.dataproviders.hu_mobiliti_ev import classify_operator
        for name in ('MOL', 'OMV Hungária Kft.', 'HU*IOY'):
            self.assertIsNone(classify_operator(name))


class TestMobilitiPrivate(unittest.TestCase):
    def test_private_usage_gets_the_private_code_but_keeps_station_ref(self):
        location = mobiliti_location('HU_MMV_9', 47.5, 19.0, [])
        location.update({'name': 'Budapest, BOSCH, 10587 - Mobiliti', 'city': 'Budapest', 'address': 'Fő utca 1.',
                         'usageType': 'PRIVATE'})
        audi = mobiliti_location('HU_AUD_3', 47.6, 19.1, [], operator='HU*AUD', postal_code='9027')
        audi.update({'name': 'Győr, Audi', 'city': 'Győr', 'address': 'Audi út 1', 'usageType': 'PRIVATE'})
        rows = run_process(sys.modules[hu_mobiliti_ev.__module__], hu_mobiliti_ev,
                           json.dumps([location, audi]), loader='save_downloaded_soup')
        self.assertEqual(['hupvtchs', 'hupvtchs'], [r['code'] for r in rows])
        self.assertEqual(['10587'], [r['ref'] for r in rows if 'ref' in r])
        # (the recording fake does not reset fields between rows, so only check that the name was set)
        self.assertIn('Audi', [r.get('name') for r in rows])


class TestUniqueNameDistance(unittest.TestCase):
    def test_distance_separates_same_town_from_other_end_of_country(self):
        from osm_poi_matchmaker.dao.poi_base import distance_m
        from osm_poi_matchmaker.utils import config
        limit = config.get_geo_unique_name_max_distance()
        # Rábatamási POI vs the OSM node it was matched to in the 2026-10 run (360 km away)
        self.assertGreater(distance_m(17.17074, 47.58608, 21.9493676, 48.141071), 300000)
        self.assertGreater(distance_m(17.17074, 47.58608, 21.9493676, 48.141071), limit)
        self.assertEqual(3000, limit)
        # a few hundred meters is the same place
        self.assertLess(distance_m(19.0310798, 47.49138709, 19.0310806, 47.49140065), limit)


class TestRefConflictExclusion(unittest.TestCase):
    def _captured_sql(self, exclude_other_ref, require_ref_network=False, name='foxpost'):
        import geopandas as gpd
        from osm_poi_matchmaker.dao.poi_base import POIBase
        captured = []

        def fake_from_postgis(query, conn, geom_col='way', params=None):
            captured.append((str(query), params))
            return gpd.GeoDataFrame()

        base = POIBase.__new__(POIBase)
        base.one_session = None  # __del__ closes it
        with patch.object(gpd.GeoDataFrame, 'from_postgis', side_effect=fake_from_postgis):
            base._query_osm_shop_poi_gpd_impl(
                MagicMock(), 19.0, 47.5, 'vending_machine_parcel_locker_and_mail_in', name, None, None,
                'ref', 'hu6002', None, None, None, None, 300, 100, 10, True, None,
                exclude_other_ref=exclude_other_ref, require_ref_network=require_ref_network)
        return captured

    def test_other_hu_ref_is_excluded_for_foxpost(self):
        captured = self._captured_sql(True)
        named = [c for c in captured if "(^|;)hu[0-9]+(;|$)" in c[0]]
        self.assertTrue(named)
        self.assertTrue(all(params.get('conflict_ref') == 'hu6002' for _, params in named))

    def test_not_applied_unless_requested(self):
        captured = self._captured_sql(False)
        self.assertFalse([c for c in captured if "(^|;)hu[0-9]+(;|$)" in c[0]])

    def test_only_foxpost_types_are_listed(self):
        from osm_poi_matchmaker.dao.poi_base import REF_CONFLICT_POI_CODES
        self.assertEqual(('hufoxpocso', 'hufoxpzcso'), REF_CONFLICT_POI_CODES)

    def test_ref_stage_requires_the_searched_network(self):
        captured = self._captured_sql(False, require_ref_network=True, name='gls')
        ref_stage = [c for c in captured if 'WITH ADDITIONAL REF' in c[0]]
        self.assertTrue(ref_stage)
        sql = ref_stage[0][0]
        self.assertIn('TEXT(operator) ILIKE :name_0', sql)
        self.assertIn("COALESCE(TEXT(operator), '') = ''", sql)
        self.assertEqual('%gls%', ref_stage[0][1]['name_0'])

    def test_ref_stage_unrestricted_for_other_providers(self):
        captured = self._captured_sql(False, require_ref_network=False, name='gls')
        ref_stage = [c for c in captured if 'WITH ADDITIONAL REF' in c[0]]
        self.assertTrue(ref_stage)
        self.assertNotIn('TEXT(operator)', ref_stage[0][0])
