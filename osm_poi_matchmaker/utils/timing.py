# -*- coding: cp1250 -*-

try:
    import datetime
    import time
except ImportError as err:
    print("Error import module: " + str(err))
    exit(128)

__author__ = 'kszalai'


class Timing:
    def __init__(self):
        self.start = time.clock()

    def end(self):
        elapsed = time.clock() - self.start
        # return self.__seconds_to_str(elapsed)
        return str(datetime.timedelta(seconds=elapsed))

    def __seconds_to_str(self, t):
        return "%d:%02d:%02d.%03d" % \
               reduce(lambda ll, b: divmod(ll[0], b) + ll[1:],
                      [(t * 1000,), 1000, 60, 60])
