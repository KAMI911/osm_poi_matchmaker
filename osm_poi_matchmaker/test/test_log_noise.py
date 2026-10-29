# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import unittest
    from unittest.mock import patch
    from osm_poi_matchmaker.libs.hu import hu_address_parser
    from osm_poi_matchmaker.libs.waxeye_process import waxeye_process
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestWaxeyeEmptyMarkerNodes(unittest.TestCase):
    def test_floor_markers_do_not_warn_and_address_is_unchanged(self):
        for floor in ('fszt.', 'földszint', 'félemelet'):
            with self.subTest(floor=floor):
                parsed = hu_address_parser.Parser().parse('1011 Budapest, Fő utca 1. {}'.format(floor))
                with self.assertNoLogs(level='WARNING'):
                    result = waxeye_process(parsed)
                self.assertEqual({'postcode': '1011', 'cTown': 'Budapest', 'type': 'utca', 'cStreet': 'Fő',
                                  'houseNumber': '1'}, result)


class TestDefaultCredentialWarning(unittest.TestCase):
    def test_warns_once_per_process(self):
        with patch.object(config, '_warned_default_credentials', set()), \
                patch.object(config, 'get_config_string', return_value='poi'), \
                patch.dict('os.environ', clear=False):
            with self.assertLogs(level='WARNING') as logs:
                for _ in range(5):
                    config.get_database_writer_username()
            self.assertEqual(1, len(logs.records))


if __name__ == '__main__':
    unittest.main()
