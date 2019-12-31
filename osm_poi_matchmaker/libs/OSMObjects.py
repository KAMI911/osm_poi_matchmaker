# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    from sys import exit
    from osm_poi_matchmaker.libs.osm import timestamp_now
    from osm_poi_matchmaker.dao.data_structure import OSM_object_type
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)


class OSMGeneral(object):

    def __init__(self, id = -1, version = 9999, user = 'kami911', uid = '8635934', timestamp = timestamp_now(), tags = {}):
        self.id = id
        self.version = version
        self.user = user
        self.uid = uid
        self.timestamp = timestamp
        self.type = None
        self.tags = tags


class OSMNode(OSMGeneral):

    def __init__(self, id = -1, version = 9999, user = 'kami911', uid = '8635934', timestamp = timestamp_now(), tags = {}, lat = None, lon = None):
        self.id = id
        self.version = version
        self.user = user
        self.uid = uid
        self.timestamp = timestamp
        self.tags = tags
        self.lat = lat
        self.lon = lon
        self.type = OSM_object_type.node


class OSMWay(OSMGeneral):

    def __init__(self, id = -1, version = 9999, user = 'kami911', uid = '8635934', timestamp = timestamp_now(), tags = {}, nodes = []):
        self.id = id
        self.version = version
        self.user = user
        self.uid = uid
        self.timestamp = timestamp
        self.tags = tags
        self.nodes = nodes
        self.type = OSM_object_type.way


class OSMList(object):

    def __init__(self, nodes = {}, ways = {}):
        self.nodes = nodes
        self.ways = ways

    def append_node(self, id, osm_node):
        self.nodes[id] = osm_node

    def append_way(self, id, osm_way):
        self.ways[id] = osm_way

    def print(self):
        print(self.nodes)
        print(self.ways)
        for key, value in self.nodes.items():
            print (key)
        for key, value in self.ways.items():
            print (key)
