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
    import numpy as np
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


def _resolve_new_value(new_value):
    """Resolve a patch ``new_*`` value: None if it's empty/missing, else the
    stripped string. Callers treat WILDCARD (kept as-is) separately."""
    if new_value is None:
        return None
    try:
        if pd.isna(new_value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(new_value).strip()
    if text.lower() in _EMPTY_STRINGS:
        return None
    return text


def _normalized_column(df, col) -> pd.Series:
    """Vectorized equivalent of calling _normalize() on every value of one
    column: stripped string, with missing/None/'none'/'nan'/'null' collapsed
    to ''. If `col` isn't a column of `df`, returns an all-'' Series, matching
    what _normalize() on a missing value would give for every row.
    """
    if col not in df.columns:
        return pd.Series([''] * len(df), index=df.index)
    s = df[col].astype('string').str.strip()
    s = s.mask(s.str.lower().isin(_EMPTY_STRINGS), '')
    return s.fillna('')


def apply_poi_patches(poi_df, patch_df):
    """Return a new dataframe with poi_patch rules applied.

    Only the first matching patch row is applied to each POI. The POI
    dataframe must have a ``poi_code`` column plus the address columns
    listed in :data:`PATCH_FIELD_MAP`; missing columns are ignored.

    Vectorized over POI rows: instead of testing every (POI, patch) pair in
    a Python-level double loop - O(len(poi_df)) * O(len(patch_df)), which
    dominates STAGE 6's runtime on realistic data (tens of thousands of POIs
    against thousands of patch rows) - this normalizes each relevant POI
    column once, then for every patch row (a much smaller set) builds one
    vectorized boolean mask over all still-unpatched POI rows and writes the
    new_* values in a single indexed assignment. An `unpatched` mask carried
    across patch rows preserves "first matching patch wins".
    """
    if poi_df is None or len(poi_df) == 0:
        return poi_df
    if patch_df is None or len(patch_df) == 0:
        return poi_df

    result = poi_df.copy()
    norm_cols = {
        'poi_code': _normalized_column(result, 'poi_code'),
        **{poi_col: _normalized_column(result, poi_col) for poi_col in PATCH_FIELD_MAP},
    }
    unpatched = np.ones(len(result), dtype=bool)
    patched = 0

    for patch_row in patch_df.to_dict('records'):
        mask = unpatched
        patch_code = _normalize(patch_row.get('poi_code'))
        if patch_code != WILDCARD:
            mask = mask & (norm_cols['poi_code'].to_numpy() == patch_code)
            if not mask.any():
                continue
        for poi_col, (orig_col, _) in PATCH_FIELD_MAP.items():
            orig = _normalize(patch_row.get(orig_col))
            if orig == WILDCARD:
                continue
            mask = mask & (norm_cols[poi_col].to_numpy() == orig)
            if not mask.any():
                break
        if not mask.any():
            continue

        for poi_col, (_, new_col) in PATCH_FIELD_MAP.items():
            if poi_col not in result.columns:
                continue
            new_value = patch_row.get(new_col)
            if _normalize(new_value) == WILDCARD:
                continue  # keep the POI's existing value
            result.loc[mask, poi_col] = _resolve_new_value(new_value)

        patched += int(mask.sum())
        unpatched = unpatched & ~mask

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
