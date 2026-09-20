# -*- coding: utf-8 -*-

"""Regression test for a real production bug found in hu_ofotert.py:
types() declared poi_code 'ofoteopt' (used to create/find its poi_common row),
but process() tagged every harvested row with self.data.code = 'huofoteopt' -
a different string. insert_poi_dataframe() resolves poi_common_id by looking
the row's poi_code up in a dict keyed by every *declared* poi_code
(dao/data_handlers.py's common_lookup); a code process() invented that types()
never declared always misses that lookup, so every row silently got inserted
with poi_common_id left NULL. Those rows then never survive STAGE 5's inner
join (poi_addr_data merged with poi_common_data on poi_common_id == pc_id), so
the provider harvested and "successfully inserted" (0 errors) 100 real rows on
every run, while producing zero output files, forever.

This is exactly the kind of copy-paste/typo bug that's invisible in normal
testing (no exception anywhere) and only shows up as "why does this provider
never produce output" - so guard it structurally: every literal poi_code a
provider's process() ever assigns to self.data.code must be one its own
types() actually declared.
"""

try:
    import unittest
    import logging
    import re
    import sys
    from pathlib import Path
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

DATAPROVIDERS_DIR = Path(__file__).resolve().parent.parent / 'dataproviders'

DECLARED_RE = re.compile(r"'poi_code'\s*:\s*'([^']+)'")
# Matches both `self.data.code = '...'` and a locally-aliased `data.code = '...'`
# (e.g. hu_cib_bank.py's `data = POIDatasetRaw(); data.code = '...'`).
USED_RE = re.compile(r"(?:self\.)?data\.code\s*=\s*'([^']+)'")


def _find_mismatches():
    """Returns {provider_filename: sorted[unused poi_code, ...]} for every
    dataprovider that assigns a self.data.code its own types() never declared.
    Skips the attic/ (retired providers, not wired into any run) and any file
    that assigns no literal poi_code at all (e.g. a dynamically-computed one,
    like hu_alltheplaces.py's per-feature hash - outside this check's scope)."""
    mismatches = {}
    for path in sorted(DATAPROVIDERS_DIR.glob('hu_*.py')):
        text = path.read_text(encoding='utf-8')
        declared = set(DECLARED_RE.findall(text))
        used = set(USED_RE.findall(text))
        if not used:
            continue
        unused = used - declared
        if unused:
            mismatches[path.name] = sorted(unused)
    return mismatches


class TestDataproviderPoiCodesMatchDeclared(unittest.TestCase):

    def test_every_assigned_poi_code_was_declared_in_types(self):
        mismatches = _find_mismatches()
        self.assertEqual(mismatches, {},
                         'process() assigns a self.data.code that types() never declared as a '
                         "poi_code (so insert_poi_dataframe()'s poi_common_id lookup always misses "
                         'and the row is silently orphaned forever): {}'.format(mismatches))

    def test_the_ofotert_bug_itself_would_have_been_caught(self):
        """Sanity-check the detector against the exact string that was live in
        hu_ofotert.py before the fix, so this test can't pass by accident (e.g.
        a regex that never matches anything real)."""
        sample = (
            "self.__types = [{'poi_code': 'ofoteopt', 'poi_common_name': 'Ofotért'}]\n"
            "self.data.code = 'huofoteopt'\n"
        )
        declared = set(DECLARED_RE.findall(sample))
        used = set(USED_RE.findall(sample))
        self.assertEqual(declared, {'ofoteopt'})
        self.assertEqual(used, {'huofoteopt'})
        self.assertTrue(used - declared, 'detector must flag this as a mismatch')


if __name__ == '__main__':
    unittest.main()
