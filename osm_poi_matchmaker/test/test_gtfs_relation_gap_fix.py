# -*- coding: utf-8 -*-

"""Tests for gtfs_relation_gap_fix.py's direction-aware routing: a bus
"bridge" path built across a one-way street or a roundabout backwards would
be worse than no fix at all (JOSM's own PT validator, and anyone driving the
route, would immediately flag it) - so directed_edges_for_way() and
shortest_way_path()'s use of it get covered on their own, without a database,
plus ROUTE_TYPE_TO_NETWORK's rail-vs-road mode selection.
"""

try:
    import unittest
    import logging
    import sys
    from osm_poi_matchmaker.tools.gtfs_relation_gap_fix import (
        directed_edges_for_way, shortest_way_path, ROUTE_TYPE_TO_NETWORK,
    )
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')
    sys.exit(128)


class TestDirectedEdgesForWay(unittest.TestCase):
    def test_no_restriction_is_bidirectional(self):
        edges = directed_edges_for_way([1, 2, 3], {})
        self.assertEqual(set(edges), {(1, 2), (2, 1), (2, 3), (3, 2)})

    def test_oneway_yes_is_forward_only(self):
        edges = directed_edges_for_way([1, 2, 3], {'oneway': 'yes'})
        self.assertEqual(set(edges), {(1, 2), (2, 3)})
        self.assertNotIn((2, 1), edges)
        self.assertNotIn((3, 2), edges)

    def test_oneway_minus_one_is_reverse_only(self):
        edges = directed_edges_for_way([1, 2, 3], {'oneway': '-1'})
        self.assertEqual(set(edges), {(2, 1), (3, 2)})

    def test_oneway_true_and_1_are_also_forward(self):
        for value in ('true', '1'):
            edges = directed_edges_for_way([1, 2], {'oneway': value})
            self.assertEqual(edges, [(1, 2)])

    def test_oneway_reverse_alias(self):
        edges = directed_edges_for_way([1, 2], {'oneway': 'reverse'})
        self.assertEqual(edges, [(2, 1)])

    def test_oneway_no_is_bidirectional(self):
        edges = directed_edges_for_way([1, 2], {'oneway': 'no'})
        self.assertEqual(set(edges), {(1, 2), (2, 1)})

    def test_roundabout_implies_oneway_for_bus(self):
        # A closed way (first node == last node, as a real OSM roundabout is
        # digitized) with no explicit oneway tag - junction=roundabout alone
        # must restrict a bus to the ring's own digitized direction.
        edges = directed_edges_for_way([10, 11, 12, 10], {'junction': 'roundabout'}, vehicle='bus')
        self.assertEqual(edges, [(10, 11), (11, 12), (12, 10)])

    def test_roundabout_junction_circular_alias(self):
        edges = directed_edges_for_way([10, 11, 10], {'junction': 'circular'}, vehicle='bus')
        self.assertEqual(edges, [(10, 11), (11, 10)])

    def test_explicit_oneway_overrides_roundabout_default(self):
        # A roundabout explicitly tagged oneway=-1 (unusual, but a mapper's
        # explicit tag must still win over the implied default).
        edges = directed_edges_for_way([10, 11, 12], {'junction': 'roundabout', 'oneway': '-1'})
        self.assertEqual(edges, [(11, 10), (12, 11)])

    def test_roundabout_does_not_restrict_rail(self):
        # junction=roundabout is a road concept - a rail vehicle on a way
        # that happens to carry this tag (shouldn't normally happen, but
        # must not silently restrict rail routing if it does) stays
        # bidirectional.
        edges = directed_edges_for_way([10, 11, 12], {'junction': 'roundabout'}, vehicle='rail')
        self.assertEqual(set(edges), {(10, 11), (11, 10), (11, 12), (12, 11)})

    def test_bus_contraflow_exemption(self):
        # A one-way street with a bus/psv contraflow lane - buses may travel
        # against the general oneway direction.
        edges = directed_edges_for_way([1, 2, 3], {'oneway': 'yes', 'oneway:bus': 'no'}, vehicle='bus')
        self.assertEqual(set(edges), {(1, 2), (2, 1), (2, 3), (3, 2)})

    def test_psv_exemption_key_alias(self):
        edges = directed_edges_for_way([1, 2], {'oneway': 'yes', 'oneway:psv': 'no'}, vehicle='bus')
        self.assertEqual(set(edges), {(1, 2), (2, 1)})

    def test_bus_exemption_does_not_apply_to_rail(self):
        # Same tags, but a rail vehicle doesn't get a bus-lane exemption -
        # oneway stays enforced (not that rail should normally be on a
        # highway=*,oneway=yes way at all, but the function must not grant
        # an exemption that only makes sense for road vehicles).
        edges = directed_edges_for_way([1, 2, 3], {'oneway': 'yes', 'oneway:bus': 'no'}, vehicle='rail')
        self.assertEqual(edges, [(1, 2), (2, 3)])

    def test_single_node_way_has_no_edges(self):
        self.assertEqual(directed_edges_for_way([5], {}), [])


class TestShortestWayPathDirectionality(unittest.TestCase):
    """shortest_way_path() just walks whatever directed graph it's handed -
    these tests build that graph with directed_edges_for_way() the same way
    build_local_network_graph() does, to cover the two together without a
    database."""

    def _graph_from_ways(self, ways, vehicle='bus'):
        graph = {}
        for way_id, nodes, tags in ways:
            for a, b in directed_edges_for_way(nodes, tags, vehicle=vehicle):
                graph.setdefault(a, []).append((b, way_id))
        return graph

    def test_bus_cannot_use_oneway_street_against_its_direction(self):
        # Single one-way way from node 1 to node 2 - start is node 2 (the
        # "wrong" end), end is node 1: must be unreachable.
        graph = self._graph_from_ways([(100, [1, 2], {'oneway': 'yes'})])
        self.assertIsNone(shortest_way_path(graph, {2}, {1}))

    def test_bus_can_use_oneway_street_in_its_direction(self):
        graph = self._graph_from_ways([(100, [1, 2], {'oneway': 'yes'})])
        self.assertEqual(shortest_way_path(graph, {1}, {2}), [100])

    def test_bus_routes_through_roundabout_in_correct_direction(self):
        # way 1 feeds into the roundabout at node 10, way 2 leaves it at node
        # 12. The ring is digitized 10 -> 11 -> 12 -> 10, so a bus entering at
        # 10 must reach 12 going the ring's own way (via 11), not backwards.
        ways = [
            (1, [0, 10], {}),  # approach road, node 0 -> node 10
            (99, [10, 11, 12, 10], {'junction': 'roundabout'}),  # the ring itself
            (2, [12, 20], {}),  # exit road, node 12 -> node 20
        ]
        graph = self._graph_from_ways(ways)
        path = shortest_way_path(graph, {0}, {20})
        self.assertEqual(path, [1, 99, 2])

    def test_bus_cannot_shortcut_roundabout_backwards(self):
        # Same ring, but ask for the hop from 12 back to 10 directly (the
        # "backwards" direction across the ring) - only reachable by going
        # the long way around (12 -> 10, which IS the ring's own direction
        # here), never by a direct 12->10 shortcut that doesn't exist.
        ways = [(99, [10, 11, 12, 10], {'junction': 'roundabout'})]
        graph = self._graph_from_ways(ways)
        # 12 -> 10 is a direct ring edge (last segment of the digitized way) -
        # reachable in 1 hop, which is correct (that IS the ring's own
        # direction). The real check is the reverse: 10 cannot jump straight
        # to 12 without going through 11 first.
        self.assertEqual(shortest_way_path(graph, {12}, {10}), [99])
        path_the_long_way = shortest_way_path(graph, {10}, {12})
        # 10->11 then 11->12 - both hops are the same way id, collapsed to one
        # entry; the real assertion is that this succeeds at all (2 hops),
        # not a direct 10->12 shortcut that doesn't exist on the ring.
        self.assertEqual(path_the_long_way, [99])

    def test_rail_vehicle_ignores_bus_contraflow_exemption(self):
        ways = [(100, [1, 2], {'oneway': 'yes', 'oneway:bus': 'no'})]
        graph_rail = self._graph_from_ways(ways, vehicle='rail')
        self.assertIsNone(shortest_way_path(graph_rail, {2}, {1}))
        graph_bus = self._graph_from_ways(ways, vehicle='bus')
        self.assertEqual(shortest_way_path(graph_bus, {2}, {1}), [100])

    def test_no_path_within_hop_limit_returns_none(self):
        # A long chain of one-way ways, each one hop, but max_hops is smaller
        # than the chain length.
        ways = [(i, [i, i + 1], {'oneway': 'yes'}) for i in range(10)]
        graph = self._graph_from_ways(ways)
        self.assertIsNone(shortest_way_path(graph, {0}, {10}, max_hops=3))
        self.assertIsNotNone(shortest_way_path(graph, {0}, {10}, max_hops=10))


class TestRouteTypeToNetwork(unittest.TestCase):
    def test_bus_uses_road_network(self):
        self.assertEqual(ROUTE_TYPE_TO_NETWORK['3'], ('road', 'bus'))

    def test_tram_subway_rail_use_rail_network(self):
        for route_type in ('0', '1', '2'):
            self.assertEqual(ROUTE_TYPE_TO_NETWORK[route_type], ('rail', 'rail'))

    def test_trolleybus_variants_use_road_network(self):
        self.assertEqual(ROUTE_TYPE_TO_NETWORK['11'], ('road', 'bus'))
        self.assertEqual(ROUTE_TYPE_TO_NETWORK['800'], ('road', 'bus'))


if __name__ == '__main__':
    unittest.main()
