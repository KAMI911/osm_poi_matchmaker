# -*- coding: utf-8 -*-

try:
    import pandas as pd
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def has_value(value) -> bool:
    """True if value is a real, present value - not None, not a pandas/numpy NaN
    float, not pandas.NaT, and not pandas.NA.

    A DataFrame column a data provider never set (or one reset back to "unmatched"
    by match_conflict_resolution.py) defaults to one of these missing-value
    sentinels depending on the column's dtype - never necessarily a plain None -
    so a plain `is not None` / truthy check lets it through, and it ends up written
    out as the literal string 'nan'/'NaT' in OSM tags, or (for a datetime column)
    crashes strftime-based formatting outright. pandas.isna() is the one check that
    already understands every one of those sentinels; this wraps it to also treat
    a blank/whitespace-only string as "no value", and to never itself raise on a
    container (list/tuple/dict/set), where pandas.isna() returns an elementwise
    array instead of a single bool.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return True
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return True
