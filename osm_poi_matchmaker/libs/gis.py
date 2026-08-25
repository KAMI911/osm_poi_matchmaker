# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    from scipy.spatial import distance
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def closest_point(point, points):
    """Return the item of `points` closest to `point` (Euclidean distance).

    Args:
        point: A single (x, y)-like coordinate.
        points: Sequence of (x, y)-like coordinates to search.

    Returns:
        The closest point from `points`.
    """
    # Find closest point from a list of points
    pt = points[distance.cdist([point], points).argmin()]
    return pt


def closest_point_distance(point, points):
    """Return the Euclidean distance from `point` to its closest match in `points`,
    formatted as a fixed-width decimal string.

    Args:
        point: A single (x, y)-like coordinate.
        points: Sequence of (x, y)-like coordinates to search.

    Returns:
        str: Distance formatted as '%10.8f'.
    """
    # Find closest point from a list of points
    pt = points[distance.cdist([point], points).argmin()]
    pt_dist = '{:10.8f}'.format(distance.euclidean(point, pt))
    return pt_dist


def match_value(df, col1, x, col2):
    """Look up the col2 value of the first row where df[col1] == x.

    Args:
        df (pd.DataFrame): Table to search.
        col1 (str): Column to match `x` against.
        x: Value to look for in col1.
        col2 (str): Column to read the result from.

    Returns:
        The col2 value of the first matching row.
    """
    # Match value x from col1 row to value in col2
    return df[df[col1] == x][col2].values[0]


def finding_closest(data1, data2):
    """For each 'point' in data2, find its closest 'point' in data1 and copy over
    that row's stop_id/stop_name - a GTFS-style nearest-stop matching helper.

    Not called anywhere in this codebase currently.

    Args:
        data1 (pd.DataFrame): Reference points, with 'point', 'stop_id' and
            'stop_name' columns.
        data2 (pd.DataFrame): Points to match, with a 'point' column. Gains
            'closest', 'dist_closest', 'stop_id' and 'stop_name' columns.

    Returns:
        pd.DataFrame: data2 with the new columns added.
    """
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
