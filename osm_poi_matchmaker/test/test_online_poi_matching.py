# -*- coding: utf-8 -*-

try:
    import unittest
    from unittest.mock import MagicMock, patch
    import logging
    import sys
    import math
    import pandas as pd
    from osm_poi_matchmaker.libs import online_poi_matching as opm
    from osm_poi_matchmaker.libs.online_poi_matching import smart_postcode_check, ordered_postcode_check
    from osm_poi_matchmaker.dao.poi_array_structure import POI_ADDR_COLS, OSM_ADDR_COLS
    from osm_poi_matchmaker.dao.data_structure import OSM_object_type
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestSmartOnlinePOIMatching(unittest.TestCase):
    def setUp(self):
        self.addresses = [
            pd.Series(data=['9737', 'Bük', 'Petőfi utca', '63', None], index=POI_ADDR_COLS),
            pd.Series(data=['9737', 'Bük', 'Petőfi utca', '63', None], index=POI_ADDR_COLS),
            pd.Series(data=['9737', 'Bük', 'Petőfi utca', '63', None], index=POI_ADDR_COLS),
            pd.Series(data=['9737', 'Bük', 'Petőfi utca', '63', None], index=POI_ADDR_COLS),
            pd.Series(data=['9737', 'Bük', 'Petőfi utca', '63', None], index=POI_ADDR_COLS),
            pd.Series(data=['9737', 'Bük', 'Petőfi utca', '63', None], index=POI_ADDR_COLS),
            pd.Series(data=['9737', 'Bük', 'Petőfi utca', '63', None], index=POI_ADDR_COLS),
            pd.Series(data=['1029', 'Budapest', 'Hidegkúti út', '1', None], index=POI_ADDR_COLS),
            pd.Series(data=['1029', 'Budapest', 'Hidegkúti út', '1', None], index=POI_ADDR_COLS),
            pd.Series(data=['1029', 'Budapest', 'Hidegkúti út', '1', None], index=POI_ADDR_COLS),
            pd.Series(data=['1028', 'Budapest', 'Hidegkúti út', '1', None], index=POI_ADDR_COLS),
            pd.Series(data=['5662', 'Csanádapáca', None, None, None], index=POI_ADDR_COLS),
            pd.Series(data=['1036', 'Budapest', 'Bécsi út', '136', None], index=POI_ADDR_COLS),
            pd.Series(data=['1024', 'Budapest', '', '', None], index=POI_ADDR_COLS),
            # Regression case: a pandas/numpy NaN poi_postcode (e.g. a provider field
            # that was never set) used to survive ordered_postcode_check()'s
            # 'is not None' guard and come back as the literal text 'nan' instead of
            # None - see TestOrderedPostcodeCheck below for the focused unit tests.
            pd.Series(data=[float('nan'), 'Teszt', 'Teszt utca', '1', None], index=POI_ADDR_COLS),
        ]
        self.osm_addresses = pd.DataFrame(
            [
                ['9737', 'Bük', 'Petőfi utca', '63', None],
                ['9737', 'Bük', 'Petőfi utca', '63', None],
                ['9737', 'Bük', 'Kossuth utca', '63', None],
                ['9737', 'Bük', 'Petőfi utca', '11', None],
                ['9738', 'Bük', 'Petőfi utca', '63', None],
                ['9738', 'Bük', 'Kossuth utca', '63', None],
                ['9738', 'Bük', 'Petőfi utca', '11', None],
                ['1028', 'Budapest', 'Hidegkúti út', '1', None],
                ['1028', 'Budapest', 'Hidegkúti tér', '1', None],
                ['1029', 'Budapest', 'Hidegkúti út', '1', None],
                ['1028', 'Budapest', 'Hidegkúti út', '1', None],
                ['5662', 'Csanádapáca', None, None, None],
                ['1032', 'Budapest', 'Bécsi út', '136', None],
                ['0', 'Budapest', '', '', None],
                [None, 'Teszt', 'Teszt utca', '1', None],
            ])
        '''
        ['5662', 'Csanádapáca', None, None, None],
        ['2463', 'Tordas', 'Köztársaság utca', '8/A', None],
        ['2000', 'Szentendre', 'Vasvári Pál utca', None, '2794/16'],
        '''
        self.osm_addresses.columns = OSM_ADDR_COLS
        self.postcodes = ['9737', '9739', '9740', '9741', '9737', '9742', '9750', '1029', '1040', '1030',
                          '1029', '5555', '1037', '0', '0']
        self.good_codes = ['9737', '9737', '9737', '9737', '9738', '9738', '9738', '1028', '1028', '1029',
                           '1028', '5662', '1032', '1024', None]

    def test_smart_online_poi_matching(self):
        case = 0
        for i in range(0, len(self.addresses)):
            case += 1
            postcode = smart_postcode_check(self.addresses[i], self.osm_addresses.iloc[[i]], self.postcodes[i])
            with self.subTest():
                self.assertEqual(postcode, self.good_codes[i], 'Case {}'.format(case))


class TestOrderedPostcodeCheck(unittest.TestCase):
    """Regression tests for ordered_postcode_check()'s NaN handling.

    A pandas/numpy NaN float compares unequal to everything under IEEE 754
    (nan != 0 and nan != '0' are both True), so a plain 'postcode is not None'
    guard let a NaN postcode through, and str(nan) returned the literal text
    'nan' as if it were a real postcode - which then ended up written into
    poi_postcode and, from there, into the OSM XML/CSV output as the literal
    string 'nan'. See has_value() usage in ordered_postcode_check().
    """

    def test_all_nan_returns_none(self):
        nan = float('nan')
        self.assertIsNone(ordered_postcode_check([nan, nan, nan]))

    def test_nan_is_skipped_in_favour_of_a_real_value(self):
        nan = float('nan')
        self.assertEqual(ordered_postcode_check([nan, None, '1026']), '1026')
        self.assertEqual(ordered_postcode_check([nan, nan, '1026']), '1026')

    def test_nan_never_stringified(self):
        nan = float('nan')
        result = ordered_postcode_check([nan, nan, nan])
        self.assertNotEqual(result, 'nan')

    def test_none_and_zero_variants_are_skipped(self):
        self.assertIsNone(ordered_postcode_check([None, 0, '0']))
        self.assertEqual(ordered_postcode_check([None, 0, '6720']), '6720')

    def test_first_real_value_wins_priority_order(self):
        self.assertEqual(ordered_postcode_check(['1026', '9737', '1024']), '1026')

    def test_numeric_postcode_is_stringified(self):
        self.assertEqual(ordered_postcode_check([1026]), '1026')

    def test_empty_list_returns_none(self):
        self.assertIsNone(ordered_postcode_check([]))


def _candidate_row(**overrides):
    """One-row DataFrame with every column find_osm_matches()/enrich_matched_pois()
    read from `row` in the code paths these tests exercise."""
    base = dict(
        poi_ref='hu04', additional_ref_name='ref', poi_additional_ref=None,
        poi_lon=19.0, poi_lat=47.5, poi_common_id=1,
        poi_search_name='aldi', poi_search_avoid_name=None, poi_name='Aldi',
        poi_addr_street='Mészáros utca', poi_addr_housenumber='56',
        poi_conscriptionnumber=None, poi_city='Budapest', poi_postcode='1016',
        osm_search_distance_perfect=600, osm_search_distance_safe=200,
        osm_search_distance_unsafe=2, do_not_export_addr_tags=False,
        preserve_original_post_code=True, poi_type='shop', poi_common_name='Aldi',
        osm_id=None, osm_node=None, poi_new=False, _changed_from_osm=False,
        poi_distance=None, osm_live_tags=None, osm_lat=None, osm_lon=None,
    )
    base.update(overrides)
    return pd.DataFrame([base])


class TestFindOsmMatches(unittest.TestCase):
    """find_osm_matches() (STAGE 9 phase 1) must only ever touch the local DB
    (db.query_osm_shop_poi_gpd() and friends) - never the live OSM API - so that
    match_conflict_resolution() can settle the final osm_id<->POI assignment before
    any row pays for a live tag download it might not get to keep."""

    def setUp(self):
        self.osm_query = pd.DataFrame({
            'osm_id': [7059681581],
            'node': ['node'],
            'lat': [47.5163083],
            'lon': [19.0003085],
            'addr:postcode': ['1026'],
            'addr:city': ['Budapest'],
            'addr:street': ['Szilágyi Erzsébet fasor'],
            'addr:housenumber': ['121'],
            'addr:conscriptionnumber': [None],
            'osm_version': ['23'],
            'osm_changeset': [111],
            'osm_timestamp': ['2026-06-15T09:59:15Z'],
            'distance': [0.0],
        })
        self.mock_db = MagicMock()
        self.mock_db.query_osm_shop_poi_gpd.return_value = self.osm_query
        self.mock_db.query_ways_nodes.return_value = []
        self.mock_db.query_relation_nodes.return_value = []

        self._orig_db = opm._worker_db
        self._orig_session = opm._worker_session
        self._orig_api = opm._worker_osm_live_query
        opm._worker_db = self.mock_db
        opm._worker_session = MagicMock()
        # find_osm_matches() must never touch this - any call is a test failure.
        opm._worker_osm_live_query = MagicMock()

        self.comm_data = pd.DataFrame({'pc_id': [1], 'poi_type': ['shop']})

    def tearDown(self):
        opm._worker_db = self._orig_db
        opm._worker_session = self._orig_session
        opm._worker_osm_live_query = self._orig_api

    def test_match_sets_osm_id_without_live_api_call(self):
        data = _candidate_row()
        result = opm.find_osm_matches((data, self.comm_data))
        self.assertEqual(result.at[0, 'osm_id'], 7059681581)
        self.assertEqual(result.at[0, 'osm_node'], OSM_object_type.node)
        self.assertFalse(result.at[0, 'poi_new'])
        self.assertEqual(result.at[0, 'osm_version'], '23')
        opm._worker_osm_live_query.node_get.assert_not_called()
        opm._worker_osm_live_query.way_get.assert_not_called()
        opm._worker_osm_live_query.relation_get.assert_not_called()

    def test_match_stashes_osm_coords_without_touching_poi_coords(self):
        """poi_lat/poi_lon must stay exactly the provider's own coordinate through
        this phase - match_conflict_resolution.py's resolve_conflict() needs each
        conflicting row's *own* coordinate to compute a meaningful distance to the
        (now separately stashed) osm_lat/osm_lon. Snapping poi_lat/poi_lon here
        instead would make every row matched to the same OSM element collapse onto
        identical coordinates, making that distance always ~0 for all of them."""
        data = _candidate_row(poi_lat=47.5, poi_lon=19.0)
        result = opm.find_osm_matches((data, self.comm_data))
        self.assertEqual(result.at[0, 'osm_lat'], 47.5163083)
        self.assertEqual(result.at[0, 'osm_lon'], 19.0003085)
        self.assertEqual(result.at[0, 'poi_lat'], 47.5)
        self.assertEqual(result.at[0, 'poi_lon'], 19.0)

    def test_match_with_changed_address_stashes_the_decision(self):
        """The 'Old changed ...' vs 'Old ...' log line moved to enrich_matched_pois() -
        this row's provider housenumber ('56') differs from OSM's ('121' from the
        mocked osm_query), so the bridging column must record that."""
        data = _candidate_row(poi_addr_housenumber='56')
        result = opm.find_osm_matches((data, self.comm_data))
        self.assertTrue(result.at[0, '_changed_from_osm'])

    def test_no_match_marks_new_without_touching_building_query(self):
        """Building-relocation/postcode-refinement for a genuinely-new POI now
        happens in enrich_matched_pois(), not here - a demoted row needs the exact
        same treatment later, so find_osm_matches() must not do it twice."""
        self.mock_db.query_osm_shop_poi_gpd.return_value = None
        data = _candidate_row()
        result = opm.find_osm_matches((data, self.comm_data))
        self.assertTrue(result.at[0, 'poi_new'])
        self.mock_db.query_osm_building_poi_gpd.assert_not_called()
        opm._worker_osm_live_query.node_get.assert_not_called()


class TestEnrichMatchedPois(unittest.TestCase):
    """enrich_matched_pois() (STAGE 9 phase 2) must key off the *final* (post-
    conflict-resolution) osm_id: download live tags only for a row that still has
    one, and give the 'new POI' treatment (including to a conflict-demoted row)
    otherwise."""

    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_db.query_from_cache.return_value = None
        self.mock_db.query_osm_building_poi_gpd.return_value = None

        self.mock_api = MagicMock()
        self.mock_api.node_get.return_value = {
            'tag': {'shop': 'supermarket', 'brand': 'Aldi'},
            'version': '24', 'user': 'someone', 'uid': 1,
            'changeset': 999, 'timestamp': '2026-08-01T00:00:00Z',
            'lat': 47.5163083, 'lon': 19.0003085,
        }

        self._orig_db = opm._worker_db
        self._orig_session = opm._worker_session
        self._orig_api = opm._worker_osm_live_query
        opm._worker_db = self.mock_db
        opm._worker_session = MagicMock()
        opm._worker_osm_live_query = self.mock_api

        self.comm_data = pd.DataFrame({'pc_id': [1], 'poi_type': ['shop']})
        self._cache_patcher = patch('osm_poi_matchmaker.libs.online_poi_matching.get_or_create_cache')
        self._cache_patcher.start()

    def tearDown(self):
        self._cache_patcher.stop()
        opm._worker_db = self._orig_db
        opm._worker_session = self._orig_session
        opm._worker_osm_live_query = self._orig_api

    def test_final_match_snaps_poi_coords_onto_osm_element(self):
        """The 'Using new coordinates ...' snap moved here from find_osm_matches() -
        a row that survived conflict resolution should now get its poi_lat/poi_lon
        set to the matched OSM element's own coordinate (stashed in osm_lat/osm_lon
        by find_osm_matches())."""
        data = _candidate_row(osm_id=7059681581, osm_node=OSM_object_type.node,
                              poi_lat=47.5, poi_lon=19.0,
                              osm_lat=47.5163083, osm_lon=19.0003085)
        result = opm.enrich_matched_pois((data, self.comm_data))
        self.assertEqual(result.at[0, 'poi_lat'], 47.5163083)
        self.assertEqual(result.at[0, 'poi_lon'], 19.0003085)

    def test_final_match_downloads_live_tags(self):
        data = _candidate_row(osm_id=7059681581, osm_node=OSM_object_type.node,
                              _changed_from_osm=False)
        result = opm.enrich_matched_pois((data, self.comm_data))
        self.mock_api.node_get.assert_called_once_with(7059681581)
        self.assertEqual(result.at[0, 'osm_live_tags'], {'shop': 'supermarket', 'brand': 'Aldi'})

    def test_demoted_row_gets_new_poi_treatment_not_a_live_download(self):
        """A row match_conflict_resolution() demoted has osm_id=None but poi_new
        was already flipped back to True there - enrich_matched_pois() must not
        redo that (defensive re-set is fine), must attempt the building-relocation
        query, and must never call the live OSM API for it."""
        data = _candidate_row(osm_id=None, osm_node=None, poi_new=True)
        result = opm.enrich_matched_pois((data, self.comm_data))
        self.assertTrue(result.at[0, 'poi_new'])
        self.mock_db.query_osm_building_poi_gpd.assert_called_once()
        self.mock_api.node_get.assert_not_called()
        self.mock_api.way_get.assert_not_called()
        self.mock_api.relation_get.assert_not_called()

    def test_never_matched_row_also_gets_new_poi_treatment(self):
        """Same bucket as a demoted row - find_osm_matches() never found anything,
        so osm_id was always None and poi_new was already True."""
        data = _candidate_row(osm_id=None, osm_node=None, poi_new=True)
        result = opm.enrich_matched_pois((data, self.comm_data))
        self.assertTrue(result.at[0, 'poi_new'])
        self.mock_api.node_get.assert_not_called()


class TestFindOsmMatchesFeedsConflictResolutionRealDistances(unittest.TestCase):
    """Integration test: find_osm_matches()'s output, fed into
    match_conflict_resolution.resolve_conflict(), must let it pick the actually-
    farther duplicate - not an arbitrary one because every row collapsed onto the
    same coordinate. This is the concrete bug two duplicate-harvested rows for the
    same real-world POI (e.g. two Aldi rows, ~30-60m apart) used to hit: find_osm_
    matches() snapped both onto the OSM element's coordinate, so resolve_conflict()
    saw a 0m distance for both and picked essentially arbitrarily."""

    def setUp(self):
        self.osm_query = pd.DataFrame({
            'osm_id': [7059681581], 'node': ['node'],
            'lat': [47.5163083], 'lon': [19.0003085],
            'addr:postcode': ['1026'], 'addr:city': ['Budapest'],
            'addr:street': ['Szilágyi Erzsébet fasor'], 'addr:housenumber': ['121'],
            'addr:conscriptionnumber': [None], 'osm_version': ['23'],
            'osm_changeset': [111], 'osm_timestamp': ['2026-06-15T09:59:15Z'],
            'distance': [0.0],
        })
        self.mock_db = MagicMock()
        self.mock_db.query_osm_shop_poi_gpd.return_value = self.osm_query
        self._orig_db = opm._worker_db
        self._orig_session = opm._worker_session
        self._orig_api = opm._worker_osm_live_query
        opm._worker_db = self.mock_db
        opm._worker_session = MagicMock()
        opm._worker_osm_live_query = MagicMock()
        self.comm_data = pd.DataFrame({'pc_id': [1], 'poi_type': ['shop']})

    def tearDown(self):
        opm._worker_db = self._orig_db
        opm._worker_session = self._orig_session
        opm._worker_osm_live_query = self._orig_api

    def test_the_actually_closer_duplicate_survives(self):
        from osm_poi_matchmaker.libs.match_conflict_resolution import resolve_conflict

        # Two duplicate-harvested rows for the same physical Aldi, at genuinely
        # different original coordinates - row 0 much farther from the OSM element
        # (~0.03 deg) than row 1 (a few meters).
        close_row = _candidate_row(poi_lat=47.5163083, poi_lon=19.0003085).iloc[0].to_dict()
        far_row = _candidate_row(poi_lat=47.55, poi_lon=19.03).iloc[0].to_dict()
        data = pd.DataFrame([far_row, close_row])

        matched = opm.find_osm_matches((data, self.comm_data))
        # Sanity check on the bug this guards against: without the fix, both rows'
        # poi_lat/poi_lon would already equal the OSM element's coordinate here.
        self.assertNotEqual(matched.at[0, 'poi_lat'], matched.at[1, 'poi_lat'])

        resolve_conflict(matched, 7059681581, [0, 1])

        # Row 0 (the genuinely farther one) must be the one demoted.
        self.assertTrue(pd.isna(matched.at[0, 'osm_id']))
        self.assertEqual(matched.at[1, 'osm_id'], 7059681581)
