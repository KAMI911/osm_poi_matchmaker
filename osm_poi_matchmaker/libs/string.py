# -*- coding: utf-8 -*-

try:
    import math
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def has_value(value) -> bool:
    """True if value is a real, present value - not None and not a pandas/numpy NaN float.

    A DataFrame column a data provider never set defaults to NaN, which is neither None
    nor an empty string, so plain `is not None` / truthy checks let it through and it ends
    up written out as the literal string 'nan' in OSM tags.
    """
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True

