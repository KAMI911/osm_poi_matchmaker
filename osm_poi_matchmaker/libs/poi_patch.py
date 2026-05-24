# -*- coding: utf-8 -*-
"""Apply rows from the ``poi_patch`` table to imported POI data.

For each patch row, if an imported POI's ``poi_code`` matches the patch's
``poi_code`` (``*`` is a wildcard) and the POI's address columns match
every ``orig_*`` value (``*`` is a wildcard, an empty/None entry only
matches an empty/None POI value), the POI's address fields are replaced
with the corresponding ``new_*`` values. A ``new_*`` value of ``*`` keeps
the original POI value; an empty/None ``new_*`` sets the POI value to
``None``.
"""

try:
    import logging
    import pandas as pd
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    raise


WILDCARD = '*'

# Map POI dataframe column -> (orig patch column, new patch column).
PATCH_FIELD_MAP = {
    'poi_postcode': ('orig_postcode', 'new_postcode'),
    'poi_city': ('orig_city', 'new_city'),
    'poi_addr_street': ('orig_street', 'new_street'),
    'poi_addr_housenumber': ('orig_housenumber', 'new_housenumber'),
    'poi_conscriptionnumber': ('orig_conscriptionnumber', 'new_conscriptionnumber'),
    'poi_name': ('orig_name', 'new_name'),
}

_EMPTY_STRINGS = {'', 'none', 'nan', 'null'}


def _normalize(value) -> str:
    """Return a stripped string representation; empty for missing values."""
    if value is None:
        return ''
    try:
        if pd.isna(value):
            return ''
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text.lower() in _EMPTY_STRINGS:
        return ''
    return text


def _orig_matches(poi_value, orig_value) -> bool:
    orig = _normalize(orig_value)
    if orig == WILDCARD:
        return True
    return _normalize(poi_value) == orig


def _row_matches_patch(poi_row: dict, patch_row: dict) -> bool:
    patch_code = _normalize(patch_row.get('poi_code'))
    if patch_code != WILDCARD:
        if patch_code != _normalize(poi_row.get('poi_code')):
            return False
    for poi_col, (orig_col, _) in PATCH_FIELD_MAP.items():
        if not _orig_matches(poi_row.get(poi_col), patch_row.get(orig_col)):
            return False
    return True


def _resolve_new_value(original, new_value):
    """Resolve a patch ``new_*`` value against the POI's original value."""
    if new_value is None:
        return None
    try:
        if pd.isna(new_value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(new_value).strip()
    if text == WILDCARD:
        return original
    if text.lower() in _EMPTY_STRINGS:
        return None
    return text


def apply_poi_patches(poi_df, patch_df):
    """Return a new dataframe with poi_patch rules applied.

    Only the first matching patch row is applied to each POI. The POI
    dataframe must have a ``poi_code`` column plus the address columns
    listed in :data:`PATCH_FIELD_MAP`; missing columns are ignored.
    """
    if poi_df is None or len(poi_df) == 0:
        return poi_df
    if patch_df is None or len(patch_df) == 0:
        return poi_df

    result = poi_df.copy()
    patch_records = patch_df.to_dict('records')
    patched = 0

    for idx in result.index:
        poi_row = result.loc[idx].to_dict()
        for patch_row in patch_records:
            if not _row_matches_patch(poi_row, patch_row):
                continue
            for poi_col, (_, new_col) in PATCH_FIELD_MAP.items():
                if poi_col not in result.columns:
                    continue
                resolved = _resolve_new_value(poi_row.get(poi_col), patch_row.get(new_col))
                result.at[idx, poi_col] = resolved
            patched += 1
            break

    if patched:
        logging.info('Applied %d POI patch update(s).', patched)
    else:
        logging.debug('No POI patches matched any imported POI.')
    return result


def load_poi_patches_from_db(database):
    """Load the ``poi_patch`` table as a dataframe via :class:`POIBase`."""
    try:
        return database.query_all_pd('poi_patch')
    except Exception:
        logging.exception('Failed to load poi_patch table.')
        return None
