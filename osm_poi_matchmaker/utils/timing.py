# -*- coding: cp1250 -*-

try:
    import logging
    import sys
    import datetime
    import time
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

__author__ = 'kszalai'


class Timing:
    """Simple stopwatch: start it on construction, read the elapsed time with end()."""

    def __init__(self):
        """Start the stopwatch, recording the current time as the starting point."""
        self.start = time.time()

    def end(self):
        """Return the time elapsed since construction.

        Returns:
            str: Elapsed time formatted as 'H:MM:SS.ffffff' (str(datetime.timedelta)).
        """
        elapsed = time.time() - self.start
        return str(datetime.timedelta(seconds=elapsed))
