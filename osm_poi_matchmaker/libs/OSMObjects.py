# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import datetime
    from osm_poi_matchmaker.libs.osm import timestamp_now
    from osm_poi_matchmaker.dao.data_structure import OSM_object_type
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class OSMGeneral(object):
    """Plain-object representation of a generic OSM element's header fields
    (id/version/user/uid/timestamp/tags). Not used anywhere in this codebase
    currently - the actual OSM XML export (libs/file_output.py) builds lxml
    elements/dicts directly instead of going through these classes."""

    def __init__(self, osmid: int = -1, version: int = 9999, user: str = 'kami911', uid: int = '8635934',
                 timestamp: datetime = datetime.datetime.now(), tags: dict = None):
        self.id = osmid
        self.version = version
        self.user = user
        self.uid = uid
        self.timestamp = timestamp
        self.type = None
        self.tags = tags


class OSMNode(OSMGeneral):
    """OSMGeneral plus lat/lon, representing an OSM node."""

    def __init__(self, osmid: int = -1, version: int = 9999, user: str = 'kami911', uid: int = '8635934',
                 timestamp: datetime = datetime.datetime.now(), tags: dict = None,
                 lat: float = None, lon: float = None):
        self.id = osmid
        self.version = version
        self.user = user
        self.uid = uid
        self.timestamp = timestamp
        self.tags = tags
        self.lat = lat
        self.lon = lon
        self.type = OSM_object_type.node


class OSMWay(OSMGeneral):
    """OSMGeneral plus an ordered list of member node ids, representing an OSM way."""

    def __init__(self, osmid: int = -1, version: int = 9999, user: str = 'kami911', uid: int = '8635934',
                 timestamp: datetime = datetime.datetime.now(), tags: dict = None, nodes: list = None):
        self.id = osmid
        self.version = version
        self.user = user
        self.uid = uid
        self.timestamp = timestamp
        self.tags = tags
        self.nodes = nodes
        self.type = OSM_object_type.way


class OSMList(object):
    """Container of OSMNode/OSMWay objects keyed by OSM id. nodes/ways must be
    passed in as dicts (append_node/append_way index-assign into them, they don't
    initialize a dict themselves)."""

    def __init__(self, nodes: list = None, ways: list = None):
        """Args:
            nodes (dict[int, OSMNode] | None): Initial node dict.
            ways (dict[int, OSMWay] | None): Initial way dict.
        """
        self.nodes = nodes
        self.ways = ways

    def append_node(self, osmid, osm_node):
        """Add/replace a node in self.nodes under key osmid."""
        self.nodes[osmid] = osm_node

    def append_way(self, osmid, osm_way):
        """Add/replace a way in self.ways under key osmid."""
        self.ways[osmid] = osm_way

    def print(self):
        """Debug helper: print the nodes dict, the ways dict, then each of their keys."""
        print(self.nodes)
        print(self.ways)
        for key, value in self.nodes.items():
            print(key)
        for key, value in self.ways.items():
            print(key)
