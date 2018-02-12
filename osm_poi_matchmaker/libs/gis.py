# -*- coding: utf-8 -*-

try:
    import logging
    from scipy.spatial import distance
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)

def closest_point(point, points):
    # Find closest point from a list of points
    pt = points[distance.cdist([point], points).argmin()]
    return pt


def closest_point_distance(point, points):
    # Find closest point from a list of points
    pt = points[distance.cdist([point], points).argmin()]
    pt_dist = '{:10.8f}'.format(distance.euclidean(point, pt))
    return pt_dist


def match_value(df, col1, x, col2):
    # Match value x from col1 row to value in col2
    return df[df[col1] == x][col2].values[0]


def finding_closest(data1, data2):
    # Add stop_id and stop_name to the closest point
    logging.info('Finding closest coordinates')
    data2['closest'] = [closest_point(x, list(data1['point'])) for x in data2['point']]
    logging.info('Calculating closest coordinates distances')
    data2['dist_closest'] = [closest_point_distance(x, list(data1['point'])) for x in data2['point']]
    logging.info('Selecting matching stop_id')
    data2['stop_id'] = [match_value(data1, 'point', x, 'stop_id') for x in data2['closest']]
    logging.info('Selecting matching name')
    data2['stop_name'] = [match_value(data1, 'point', x, 'stop_name') for x in data2['closest']]
    return data2