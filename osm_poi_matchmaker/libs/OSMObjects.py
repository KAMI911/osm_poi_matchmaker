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

    def __init__(self, nodes: list = None, ways: list = None):
        self.nodes = nodes
        self.ways = ways

    def append_node(self, osmid, osm_node):
        self.nodes[osmid] = osm_node

    def append_way(self, osmid, osm_way):
        self.ways[osmid] = osm_way

    def print(self):
        print(self.nodes)
        print(self.ways)
        for key, value in self.nodes.items():
            print(key)
        for key, value in self.ways.items():
            print(key)
