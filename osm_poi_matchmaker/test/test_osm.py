# -*- coding: utf-8 -*-

try:
    import unittest
    import logging
    import sys
    import json
    from osm_poi_matchmaker.libs.osm import relationer
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class TestOSMRelationer(unittest.TestCase):
    def setUp(self):
        self.test_data = [
            {'original': ['w25291279', 'outer', 'w25291280', 'inner'],
             'output': [{'ref': '25291279', 'role': 'outer', 'type': 'way'},
                        {'ref': '25291280', 'role': 'inner', 'type': 'way'}]},
            {'original': ['r555643', '', 'r555642', ''],
             'output': [{'ref': '555643', 'role': '', 'type': 'relation'},
                        {'ref': '555642', 'role': '', 'type': 'relation'}]},
        ]

    def test_relationer(self):
        for i in self.test_data:
            original, output = i['original'], i['output']
            a = relationer(original)
            with self.subTest():
                self.assertListEqual(output, a)


class TestRelationerJSONRoundtrip(unittest.TestCase):
    """Regression test for the file_output.py generate_osm_xml() relation branch.

    online_poi_matching.py stores a relation's member list into the DataFrame's
    osm_nodes column as a JSON string (json.dumps(nodes) - see its way and
    relation branches, which both do this identically). The way branch in
    file_output.py correctly json.loads()s that string back into a list before
    using it; the relation branch used to pass the raw JSON string straight to
    relationer(), which iterates its argument in (ref, role) index pairs -
    walking the *characters* of the string instead of its list elements. That
    produced <member> tags with an empty ref and a single-character role (see
    the real-world example: OSM relation 9148932, Veszprém OBI).
    """

    def test_relationer_on_raw_json_string_produces_garbage(self):
        # This documents the bug that existed before the fix: passing the
        # stored (un-parsed) JSON string to relationer() directly.
        nodes = ['w152820972', 'outer', 'w657859209', 'outer']
        osm_nodes_stored = json.dumps(nodes)
        garbage = relationer(osm_nodes_stored)
        # None of the resulting "members" carry a real ref - the ids never
        # survive being indexed one character at a time.
        self.assertTrue(all(m['ref'] == '' for m in garbage))

    def test_relationer_on_parsed_json_string_is_correct(self):
        # This is the fix: json.loads() the stored string before relationer().
        nodes = ['w152820972', 'outer', 'w657859209', 'outer']
        osm_nodes_stored = json.dumps(nodes)
        members = relationer(json.loads(osm_nodes_stored))
        self.assertListEqual(members, [
            {'type': 'way', 'ref': '152820972', 'role': 'outer'},
            {'type': 'way', 'ref': '657859209', 'role': 'outer'},
        ])
