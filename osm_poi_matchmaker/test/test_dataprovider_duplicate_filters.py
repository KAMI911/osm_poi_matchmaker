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
    from osm_poi_matchmaker.libs.file_output import keep_osm_ref_spelling
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
        self.assertEqual(['ALKOTÁS utca', 'GYŐR'], [r['branch'] for r in rows])


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
