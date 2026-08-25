# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import threading
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

_context = threading.local()


def set_current_provider(name):
    """Tag every log record emitted by this process/thread from now on with the
    given data provider module name (e.g. 'hu_mav'), until the next
    set_current_provider()/clear_current_provider() call - read by
    ProviderLogFilter, see log.conf's %(provider)s.

    STAGE 2 (POI harvest) dispatches one import_poi_data_module(module) call per
    provider to a multiprocessing.Pool; each worker only ever runs one such call at
    a time (even though a worker is reused across many providers over the pool's
    lifetime), so this plain thread-local - set once at the top of that call - stays
    correct for every log line the provider's harvest produces, including ones
    logged from shared library code (soup.py, address.py, etc.) on its behalf.
    """
    _context.provider = name or '-'


def clear_current_provider():
    """Reset the current provider tag to '-' (used for log lines outside any
    provider's harvest, e.g. pipeline-stage/setup logging)."""
    _context.provider = '-'


class ProviderLogFilter(logging.Filter):
    """Injects record.provider (see set_current_provider()) so log.conf's format
    strings can include %(provider)s. Attached to the root logger in
    create_db.py/config.py's init_log(), right after logging.config.fileConfig() -
    multiprocessing.Pool workers on Linux inherit it via fork()."""

    def filter(self, record):
        record.provider = getattr(_context, 'provider', '-')
        return True
