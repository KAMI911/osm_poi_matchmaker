# -*- coding: utf-8 -*-

__author__ = 'kami911'

try:
    import logging
    import sys
    import tracemalloc
    from osm_poi_matchmaker.utils.config import get_memory_enable_tracing
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class MemoryInfo(object):

    def __init__(self):
        self.info = tracemalloc
        self.enabled = get_memory_enable_tracing()
        if self.enabled:
            if not self.info.is_tracing():
                logging.info('Starting tracemalloc tracing.')
                self.info.start()
            else:
                logging.info('tracemalloc tracing already active.')
        else:
            logging.debug('Memory tracing disabled (memory.enable.tracing=False).')

    def log_top_memory_snapshot(self, stage_name, top_n=10):
        if not self.enabled:
            return
        if not self.info.is_tracing():
            logging.warning('tracemalloc tracing not active — starting it now.')
            self.info.start()

        snapshot = self.info.take_snapshot()
        top_stats = snapshot.statistics('lineno')

        logging.info('=== Top %d memory stats after %s ===', top_n, stage_name)
        total = 0
        for index, stat in enumerate(top_stats[:top_n], 1):
            size_in_kb = stat.size / 1024
            logging.debug('#%d: %.1f KiB %s', index, size_in_kb, stat)

            total += stat.size

        total_in_kb = total / 1024
        total_in_mb = total / (1024 * 1024)

        logging.info('=== Total allocated size of top %d: %.1f KiB (%.2f MiB) ===',
                     top_n, total_in_kb, total_in_mb)