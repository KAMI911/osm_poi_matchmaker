# -*- coding: utf-8 -*-

import unittest
from unittest.mock import MagicMock
from lxml import etree

from osm_poi_matchmaker.libs.file_output import _emit_relation_geometry


def _way_elements(root):
    return root.findall('way')


def _node_elements(root):
    return root.findall('node')


class TestEmitRelationGeometry(unittest.TestCase):
    """_emit_relation_geometry() is what gives a matched relation's member ways
    real geometry in the exported OSM XML - before this, generate_osm_xml()'s
    relation branch only ever wrote <member> references (way ids), never the
    <way>/<node> elements those ids point at, so a multipolygon (e.g. a Tesco
    building outline) rendered as an empty/incomplete shape when the exported
    file was opened standalone in JOSM."""

    def setUp(self):
        self.root = etree.Element('osm')
        self.db = MagicMock()
        self.added_nodes = []
        self.added_ways = set()
        self.added_relations = set()

    def _run(self, members):
        _emit_relation_geometry(self.root, self.db, members, self.added_nodes,
                                self.added_ways, self.added_relations)

    def test_none_members_is_a_noop(self):
        self._run(None)
        self.assertEqual(_way_elements(self.root), [])
        self.assertEqual(_node_elements(self.root), [])

    def test_emits_way_with_nd_refs_and_full_node_geometry(self):
        members = [{'type': 'way', 'ref': '287183412', 'role': 'outer'}]
        self.db.query_ways_nodes.return_value = ['100', '101', '102']
        self.db.query_nodes_coords.return_value = {
            100: (47.1, 19.1), 101: (47.2, 19.2), 102: (47.3, 19.3),
        }
        self._run(members)

        ways = _way_elements(self.root)
        self.assertEqual(len(ways), 1)
        self.assertEqual(ways[0].get('id'), '287183412')
        nd_refs = [nd.get('ref') for nd in ways[0].findall('nd')]
        self.assertEqual(nd_refs, ['100', '101', '102'])

        nodes = {n.get('id'): n for n in _node_elements(self.root)}
        self.assertEqual(set(nodes), {'100', '101', '102'})
        self.assertEqual(nodes['100'].get('lat'), '47.1')
        self.assertEqual(nodes['100'].get('lon'), '19.1')
        self.assertEqual(self.added_ways, {287183412})
        self.assertEqual(sorted(self.added_nodes), [100, 101, 102])

    def test_shared_endpoint_node_between_two_ways_is_emitted_once(self):
        """Adjacent ways in a real multipolygon ring share their endpoint nodes -
        node 100 here closes way A into way B. Regression test for a bug where
        the shared id was written as two separate <node> elements."""
        members = [
            {'type': 'way', 'ref': '1', 'role': 'outer'},
            {'type': 'way', 'ref': '2', 'role': 'outer'},
        ]
        self.db.query_ways_nodes.side_effect = lambda way_id: {
            1: ['100', '101'],
            2: ['101', '100'],
        }[way_id]
        self.db.query_nodes_coords.return_value = {100: (47.0, 19.0), 101: (47.1, 19.1)}

        self._run(members)

        node_ids = [n.get('id') for n in _node_elements(self.root)]
        self.assertEqual(sorted(node_ids), ['100', '101'])
        self.assertEqual(len(node_ids), len(set(node_ids)), 'a shared node was emitted more than once')
        # query_nodes_coords should be asked for each id only once too.
        self.db.query_nodes_coords.assert_called_once()
        requested_ids = sorted(self.db.query_nodes_coords.call_args[0][0])
        self.assertEqual(requested_ids, [100, 101])

    def test_way_already_in_added_ways_is_skipped(self):
        self.added_ways.add(287183412)
        self._run([{'type': 'way', 'ref': '287183412', 'role': 'outer'}])
        self.db.query_ways_nodes.assert_not_called()
        self.assertEqual(_way_elements(self.root), [])

    def test_node_already_in_added_nodes_is_not_refetched(self):
        self.added_nodes.append(100)
        self._run([{'type': 'node', 'ref': '100', 'role': ''}])
        self.db.query_nodes_coords.assert_not_called()
        self.assertEqual(_node_elements(self.root), [])

    def test_direct_node_member_is_emitted(self):
        members = [{'type': 'node', 'ref': '55', 'role': 'label'}]
        self.db.query_nodes_coords.return_value = {55: (47.5, 19.5)}
        self._run(members)
        nodes = _node_elements(self.root)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].get('id'), '55')

    def test_missing_way_logs_and_continues(self):
        self.db.query_ways_nodes.return_value = None
        self._run([{'type': 'way', 'ref': '404', 'role': 'outer'}])
        self.assertEqual(_way_elements(self.root), [])
        self.assertNotIn(404, self.added_ways)

    def test_missing_node_coords_logs_and_continues(self):
        self.db.query_nodes_coords.return_value = {}
        self._run([{'type': 'node', 'ref': '55', 'role': ''}])
        self.assertEqual(_node_elements(self.root), [])
        self.assertNotIn(55, self.added_nodes)

    def test_recurses_into_nested_sub_relation(self):
        """A member can itself be another relation (rare for a shop/amenity
        outline, but real - e.g. a multipolygon nested inside a boundary or
        route relation). Its own way/node geometry must still be resolved."""
        self.db.query_relation_nodes.return_value = ['w9', 'outer']
        self.db.query_ways_nodes.return_value = ['1', '2']
        self.db.query_nodes_coords.return_value = {1: (47.0, 19.0), 2: (47.1, 19.1)}

        self._run([{'type': 'relation', 'ref': '999', 'role': 'outer'}])

        self.db.query_relation_nodes.assert_called_once_with(999)
        ways = _way_elements(self.root)
        self.assertEqual(len(ways), 1)
        self.assertEqual(ways[0].get('id'), '9')
        self.assertIn(999, self.added_relations)

    def test_sub_relation_already_visited_is_not_recursed_again(self):
        self.added_relations.add(999)
        self._run([{'type': 'relation', 'ref': '999', 'role': 'outer'}])
        self.db.query_relation_nodes.assert_not_called()

    def test_stops_at_max_depth(self):
        """A chain of nested relations deeper than max_depth must not recurse
        forever (or crash) - each level's own db.query_relation_nodes() return
        value points one level deeper."""
        self.db.query_relation_nodes.side_effect = lambda rel_id: ['r{}'.format(rel_id + 1), '']

        _emit_relation_geometry(self.root, self.db, [{'type': 'relation', 'ref': '1', 'role': ''}],
                                self.added_nodes, self.added_ways, self.added_relations,
                                depth=0, max_depth=3)

        # Depths 0..3 recurse (4 calls: for refs 1, 2, 3, 4); depth 4 would be the
        # 5th call and must not happen.
        self.assertLessEqual(self.db.query_relation_nodes.call_count, 4)
        self.db.query_ways_nodes.assert_not_called()


if __name__ == '__main__':
    unittest.main()
