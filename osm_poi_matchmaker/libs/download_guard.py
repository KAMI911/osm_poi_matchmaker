# -*- coding: utf-8 -*-

"""Shared defense for every downloader in libs/ (xml.py, soup.py, pandas.py):
detect a source silently serving an HTML "under maintenance"/error page on an
otherwise-200 response in place of the real XML/JSON/CSV feed - seen live with
Magyar Posta's ZipCodes.xml/StreetTypes.xml. Callers use this to treat such a
response the same as a failed download (fall back to the previously cached
copy instead of overwriting it with garbage)."""

try:
    import logging
    import sys
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def looks_like_html(content):
    """True if `content` (str or bytes, possibly None) starts with an HTML/
    DOCTYPE marker. A non-HTML feed (XML/JSON/CSV) starting this way is a strong,
    format-agnostic signal that a maintenance/error page came back instead of the
    real data, even on an HTTP 200 - no per-source schema knowledge needed.
    """
    if not content:
        return False
    text = content.decode('utf-8', errors='ignore') if isinstance(content, bytes) else content
    return text.lstrip().lower().startswith(('<!doctype html', '<html'))
