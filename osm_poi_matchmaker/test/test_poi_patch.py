# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    import pandas as pd
    from osm_poi_matchmaker.libs.poi_patch import apply_poi_patches, PATCH_FIELD_MAP
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)


PATCH_COLUMNS = [
    'poi_code',
    'orig_postcode', 'orig_city', 'orig_street', 'orig_housenumber',
    'orig_conscriptionnumber', 'orig_name',
    'new_postcode', 'new_city', 'new_street', 'new_housenumber',
    'new_conscriptionnumber', 'new_name',
]

POI_COLUMNS = ['poi_code'] + list(PATCH_FIELD_MAP.keys())


def make_patch(**overrides):
    row = {col: '' for col in PATCH_COLUMNS}
    row['poi_code'] = '*'
    row.update(overrides)
    return row


def make_poi(**overrides):
    row = {col: None for col in POI_COLUMNS}
    row.update(overrides)
    return row


class TestApplyPoiPatches(unittest.TestCase):

    def test_replaces_address_when_all_orig_fields_match(self):
        poi = pd.DataFrame([make_poi(
            poi_code='shop_supermarket',
            poi_postcode='1012',
            poi_city='Budapest',
            poi_addr_street='Attila utca',
            poi_addr_housenumber='109',
            poi_conscriptionnumber=None,
            poi_name=None,
        )])
        patch = pd.DataFrame([make_patch(
            poi_code='*',
            orig_postcode='1012', orig_city='Budapest',
            orig_street='Attila utca', orig_housenumber='109',
            new_postcode='1012', new_city='Budapest',
            new_street='Attila út', new_housenumber='109',
        )])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Attila út')
        self.assertEqual(result.at[0, 'poi_postcode'], '1012')
        self.assertEqual(result.at[0, 'poi_addr_housenumber'], '109')

    def test_does_not_match_when_one_orig_field_differs(self):
        poi = pd.DataFrame([make_poi(
            poi_code='shop_supermarket',
            poi_postcode='1012',
            poi_city='Budapest',
            poi_addr_street='Different street',
            poi_addr_housenumber='109',
        )])
        patch = pd.DataFrame([make_patch(
            orig_postcode='1012', orig_city='Budapest',
            orig_street='Attila utca', orig_housenumber='109',
            new_street='Attila út',
        )])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Different street')

    def test_wildcard_orig_field_matches_anything(self):
        poi = pd.DataFrame([make_poi(
            poi_code='shop_supermarket',
            poi_postcode='1011', poi_city='Budapest',
            poi_addr_street='Battyhány tér',
            poi_addr_housenumber='42',
        )])
        patch = pd.DataFrame([make_patch(
            orig_postcode='1011', orig_city='Budapest',
            orig_street='Battyhány tér', orig_housenumber='*',
            new_postcode='1011', new_city='Budapest',
            new_street='Batthyány tér', new_housenumber='*',
        )])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Batthyány tér')
        # `*` in new_housenumber must preserve original.
        self.assertEqual(result.at[0, 'poi_addr_housenumber'], '42')

    def test_poi_code_must_match_when_not_wildcard(self):
        poi = pd.DataFrame([make_poi(
            poi_code='amenity_pharmacy',
            poi_postcode='1011', poi_city='Budapest',
            poi_addr_street='Battyhány tér',
        )])
        patch = pd.DataFrame([make_patch(
            poi_code='shop_supermarket',
            orig_postcode='1011', orig_city='Budapest',
            orig_street='Battyhány tér', orig_housenumber='*',
            new_street='Batthyány tér',
        )])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Battyhány tér')

    def test_poi_code_specific_match_applies_patch(self):
        poi = pd.DataFrame([make_poi(
            poi_code='shop_supermarket',
            poi_postcode='1011', poi_city='Budapest',
            poi_addr_street='Battyhány tér',
        )])
        patch = pd.DataFrame([make_patch(
            poi_code='shop_supermarket',
            orig_postcode='1011', orig_city='Budapest',
            orig_street='Battyhány tér', orig_housenumber='*',
            new_street='Batthyány tér',
        )])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Batthyány tér')

    def test_empty_orig_only_matches_empty_poi_value(self):
        poi_with_value = pd.DataFrame([make_poi(
            poi_code='c',
            poi_postcode='1011', poi_city='Budapest',
            poi_addr_street='Foo', poi_addr_housenumber='5',
        )])
        poi_empty = pd.DataFrame([make_poi(
            poi_code='c',
            poi_postcode='1011', poi_city='Budapest',
            poi_addr_street='Foo', poi_addr_housenumber=None,
        )])
        patch = pd.DataFrame([make_patch(
            orig_postcode='1011', orig_city='Budapest',
            orig_street='Foo', orig_housenumber='',
            new_street='Bar',
        )])
        self.assertEqual(apply_poi_patches(poi_with_value, patch).at[0, 'poi_addr_street'], 'Foo')
        self.assertEqual(apply_poi_patches(poi_empty, patch).at[0, 'poi_addr_street'], 'Bar')

    def test_empty_new_field_clears_value(self):
        poi = pd.DataFrame([make_poi(
            poi_code='c', poi_postcode='1011', poi_city='Budapest',
            poi_addr_street='Batthyány tér HÉV megálló',
            poi_addr_housenumber='12',
        )])
        patch = pd.DataFrame([make_patch(
            orig_postcode='1011', orig_city='Budapest',
            orig_street='Batthyány tér HÉV megálló', orig_housenumber='*',
            new_postcode='1011', new_city='Budapest',
            new_street='Batthyány tér', new_housenumber='',
        )])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Batthyány tér')
        self.assertTrue(pd.isna(result.at[0, 'poi_addr_housenumber']))

    def test_only_first_matching_patch_applied(self):
        poi = pd.DataFrame([make_poi(
            poi_code='c', poi_postcode='1011', poi_city='Budapest',
            poi_addr_street='Foo',
        )])
        patch = pd.DataFrame([
            make_patch(orig_postcode='1011', orig_city='Budapest',
                       orig_street='Foo', orig_housenumber='*', new_street='First'),
            make_patch(orig_postcode='1011', orig_city='Budapest',
                       orig_street='Foo', orig_housenumber='*', new_street='Second'),
        ])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'First')

    def test_empty_inputs_are_passthrough(self):
        poi = pd.DataFrame(columns=POI_COLUMNS)
        patch = pd.DataFrame(columns=PATCH_COLUMNS)
        self.assertTrue(apply_poi_patches(poi, patch).equals(poi))

        poi2 = pd.DataFrame([make_poi(poi_code='c', poi_addr_street='Foo')])
        # None patch_df.
        self.assertTrue(apply_poi_patches(poi2, None).equals(poi2))
        # Empty patch_df.
        self.assertTrue(apply_poi_patches(poi2, pd.DataFrame(columns=PATCH_COLUMNS)).equals(poi2))

    def test_string_none_treated_as_empty(self):
        # The DB-loaded patch table stores str(None) -> 'None' for missing fields.
        poi = pd.DataFrame([make_poi(
            poi_code='c', poi_postcode='1011', poi_city='Budapest',
            poi_addr_street='Foo', poi_addr_housenumber=None,
            poi_conscriptionnumber=None, poi_name=None,
        )])
        patch = pd.DataFrame([make_patch(
            poi_code='*',
            orig_postcode='1011', orig_city='Budapest',
            orig_street='Foo', orig_housenumber='None',
            orig_conscriptionnumber='None', orig_name='None',
            new_postcode='1011', new_city='Budapest',
            new_street='Bar', new_housenumber='None',
            new_conscriptionnumber='None', new_name='None',
        )])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Bar')
        self.assertTrue(pd.isna(result.at[0, 'poi_addr_housenumber']))

    def test_unrelated_rows_untouched(self):
        poi = pd.DataFrame([
            make_poi(poi_code='c', poi_postcode='1011', poi_city='Budapest',
                     poi_addr_street='Foo'),
            make_poi(poi_code='c', poi_postcode='9999', poi_city='Nowhere',
                     poi_addr_street='Untouched'),
        ])
        patch = pd.DataFrame([make_patch(
            orig_postcode='1011', orig_city='Budapest',
            orig_street='Foo', orig_housenumber='*',
            new_street='Bar',
        )])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Bar')
        self.assertEqual(result.at[1, 'poi_addr_street'], 'Untouched')

    def test_multiple_rows_matched_by_different_patches(self):
        # Each row should be matched and updated by its own patch rule in a
        # single apply_poi_patches() call, independently of the others.
        poi = pd.DataFrame([
            make_poi(poi_code='c', poi_postcode='1011', poi_city='Budapest',
                     poi_addr_street='Foo'),
            make_poi(poi_code='c', poi_postcode='2000', poi_city='Szentendre',
                     poi_addr_street='Bar'),
            make_poi(poi_code='c', poi_postcode='3000', poi_city='Hatvan',
                     poi_addr_street='Baz'),
        ])
        patch = pd.DataFrame([
            make_patch(orig_postcode='1011', orig_city='Budapest',
                       orig_street='Foo', orig_housenumber='*', new_street='Foo Fixed'),
            make_patch(orig_postcode='3000', orig_city='Hatvan',
                       orig_street='Baz', orig_housenumber='*', new_street='Baz Fixed'),
        ])
        result = apply_poi_patches(poi, patch)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Foo Fixed')
        self.assertEqual(result.at[1, 'poi_addr_street'], 'Bar')
        self.assertEqual(result.at[2, 'poi_addr_street'], 'Baz Fixed')

    def test_missing_poi_column_only_matches_wildcard_or_empty_orig(self):
        # poi_conscriptionnumber is intentionally left out of the POI dataframe.
        columns = [c for c in POI_COLUMNS if c != 'poi_conscriptionnumber']
        poi = pd.DataFrame([{
            'poi_code': 'c', 'poi_postcode': '1011', 'poi_city': 'Budapest',
            'poi_addr_street': 'Foo', 'poi_addr_housenumber': '5', 'poi_name': None,
        }], columns=columns)
        patch_specific = pd.DataFrame([make_patch(
            orig_postcode='1011', orig_city='Budapest', orig_street='Foo',
            orig_housenumber='5', orig_conscriptionnumber='123',
            new_street='Should not apply',
        )])
        result = apply_poi_patches(poi, patch_specific)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Foo')

        patch_wildcard = pd.DataFrame([make_patch(
            orig_postcode='1011', orig_city='Budapest', orig_street='Foo',
            orig_housenumber='5', orig_conscriptionnumber='*',
            new_street='Applied',
        )])
        result = apply_poi_patches(poi, patch_wildcard)
        self.assertEqual(result.at[0, 'poi_addr_street'], 'Applied')


class TestRealPatchFile(unittest.TestCase):
    """Load the real data/poi_patch.tsv the same way save_downloaded_pd() does in
    production, and confirm specific corrections (issue #94, plus a few rows
    restored from the pre-TSV poi_patch.csv that the TSV rewrite had dropped)
    actually apply to a POI row shaped like real imported data.
    """

    @classmethod
    def setUpClass(cls):
        import os
        patch_path = os.path.join(os.path.dirname(__file__), '..',
                                  'osm_poi_matchmaker', 'data', 'poi_patch.tsv')
        cls.patch_df = pd.read_csv(patch_path, encoding='UTF-8', sep='\t', skiprows=0)
        cls.patch_df = cls.patch_df.where(pd.notna(cls.patch_df), None)

    def _apply(self, poi_code, postcode, city, street, housenumber):
        poi = pd.DataFrame([make_poi(
            poi_code=poi_code, poi_postcode=postcode, poi_city=city,
            poi_addr_street=street, poi_addr_housenumber=housenumber,
        )])
        return apply_poi_patches(poi, self.patch_df).iloc[0]

    def test_issue_94_boconad(self):
        row = self._apply('hupostapo', '3368', 'Boconád', 'Ságvári Endre utca', '5')
        self.assertEqual(row['poi_addr_street'], 'Kossuth Lajos út')
        self.assertEqual(row['poi_addr_housenumber'], '12')

    def test_issue_94_sap(self):
        row = self._apply('hupostapo', '4176', 'Sáp', 'Derkovits Gy utca', '1')
        self.assertEqual(row['poi_addr_street'], 'Fő utca')
        self.assertEqual(row['poi_addr_housenumber'], '9')

    def test_issue_94_jeke(self):
        row = self._apply('hupostapo', '4611', 'Jéke', 'Ifjusági út', '3')
        self.assertEqual(row['poi_addr_street'], 'Dózsa György utca')
        self.assertEqual(row['poi_addr_housenumber'], '18')

    def test_restored_row_bajcsy_zsilinszky_gyor(self):
        # From the pre-TSV poi_patch.csv, dropped by the TSV rewrite; restored.
        row = self._apply('hupostapo', '9001', 'Győr', 'Bajcsy-Zsilinszky út', '46')
        self.assertEqual(row['poi_addr_street'], 'Bajcsy-Zsilinszky utca')

    def test_restored_row_oceanarok_wildcard(self):
        row = self._apply('*', '1048', 'Budapest', 'Óceánárok utca', '7')
        self.assertEqual(row['poi_addr_street'], 'Óceán-árok utca')
        # new_housenumber is '*' for this rule, meaning "keep the original".
        self.assertEqual(row['poi_addr_housenumber'], '7')

    def test_unrelated_address_still_untouched(self):
        row = self._apply('hupostapo', '1234', 'Sehol', 'Semmi utca', '1')
        self.assertEqual(row['poi_addr_street'], 'Semmi utca')
        self.assertEqual(row['poi_addr_housenumber'], '1')


if __name__ == '__main__':
    unittest.main()
