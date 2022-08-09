# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    import pandas as pd
    from osm_poi_matchmaker.libs.online_poi_matching import smart_postcode_check
    from osm_poi_matchmaker.dao.poi_array_structure import POI_ADDR_COLS, OSM_ADDR_COLS
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
            ])
        '''
        ['5662', 'Csanádapáca', None, None, None],
        ['2463', 'Tordas', 'Köztársaság utca', '8/A', None],
        ['2000', 'Szentendre', 'Vasvári Pál utca', None, '2794/16'],
        '''
        self.osm_addresses.columns = OSM_ADDR_COLS
        self.postcodes = ['9737', '9739', '9740', '9741', '9737', '9742', '9750', '1029', '1040', '1030',
                          '1029', '5555', '1037', '0']
        self.good_codes = ['9737', '9737', '9740', '9741', '9738', '9742', '9750', '1028', '1040', '1029',
                           '1028', '5662', '1032', '1024']

    def test_smart_online_poi_matching(self):
        case = 0
        for i in range(0, len(self.addresses)):
            case += 1
            postcode = smart_postcode_check(self.addresses[i], self.osm_addresses.iloc[[i]], self.postcodes[i])
            with self.subTest():
                self.assertEqual(postcode, self.good_codes[i], 'Case {}'.format(case))
