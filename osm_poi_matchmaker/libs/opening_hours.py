# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import collections
    import pandas as pd
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


class OpeningHours(object):
    """Turn 28 per-day open/close (+ optional summer open/close) time values into a
    single OSM opening_hours string, merging consecutive days with identical hours
    into ranges (e.g. 'Mo-Fr 08:00-18:00; Sa 08:00-12:00')."""

    def __init__(self, non_stop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c,
                 fr_c, sa_c, su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o,
                 summer_sa_o, summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c,
                 summer_fr_c, summer_sa_c, summer_su_c, lb_start, lb_stop, public_holiday_open=None):
        """Build the internal per-weekday table used by process().

        Args:
            non_stop (bool): If True, process() always returns '24/7' (+ PH suffix).
            mo_o..su_c (str | None): Regular open/close time per weekday, e.g. '08:00'.
            summer_mo_o..summer_su_c (str | None): Summer-season open/close per
                weekday (currently stored but not read by process()).
            lb_start (str | None): Lunch break start time, e.g. '12:00'.
            lb_stop (str | None): Lunch break end time, e.g. '12:30'.
            public_holiday_open (bool | None): True appends '; PH open', False
                appends '; PH off', None omits any PH suffix.
        """
        self.__non_stop = non_stop
        self.opening_hours = {'mo': [mo_o, mo_c, summer_mo_o, summer_mo_c, 0],
                              'tu': [tu_o, tu_c, summer_tu_o, summer_tu_c, 1],
                              'we': [we_o, we_c, summer_we_o, summer_we_c, 2],
                              'th': [th_o, th_c, summer_th_o, summer_th_c, 3],
                              'fr': [fr_o, fr_c, summer_fr_o, summer_fr_c, 4],
                              'sa': [sa_o, sa_c, summer_sa_o, summer_sa_c, 5],
                              'su': [su_o, su_c, summer_su_o, summer_su_c, 6]}
        self.__lunch_break_start = lb_start
        self.__lunch_break_stop = lb_stop
        self.week_days = {0: 'mo', 1: 'tu', 2: 'we', 3: 'th', 4: 'fr', 5: 'sa', 6: 'su'}
        self.oh_types = ('open', 'close', 'summer_open', 'summer_close', 'did')
        self.df_oh = pd.DataFrame.from_dict(self.opening_hours, orient='index', columns=self.oh_types)
        self.df_dup = self.df_oh.sort_values('did').drop_duplicates(['open', 'close'], keep='first')
        self.df_dup['same'] = None
        self.__public_holiday_open = public_holiday_open
        for row in self.df_dup.itertuples():
            same = self.df_oh.loc[
                (self.df_oh['open'] == row.open) & (self.df_oh['close'] == row.close)].index.tolist()
            if same is not None:
                same_id = self.df_oh.loc[(self.df_oh['open'] == row.open) & (self.df_oh['close'] == row.close)][
                    'did'].tolist()
                self.df_dup.at[row.Index, 'same'] = collections.OrderedDict(zip(same_id, same))

    @property
    def nonstop(self):
        """bool: Whether process() should short-circuit to '24/7' regardless of the
        per-day table."""
        return self.__non_stop

    @nonstop.setter
    def nonstop(self, value):
        self.__non_stop = value

    @property
    def public_holiday_open(self):
        """bool | None: PH suffix mode for process() - True='; PH open',
        False='; PH off', None=no suffix."""
        return self.__public_holiday_open

    @public_holiday_open.setter
    def public_holiday_open(self, value):
        self.__public_holiday_open = value

    @property
    def lunch_break_start(self) -> str:
        """Get lunch break start for opening hours

        Returns:
            str: Stored value of launch break start, value like '12:00'
        """
        return self.__lunch_break_start

    @lunch_break_start.setter
    def lunch_break_start(self, value: str):
        """Set lunch break start for opening hours

        Args:
            data (str): Store value of launch break start with value like '12:00'
        """
        self.__lunch_break_start = value

    @property
    def lunch_break_stop(self) -> str:
        """Get lunch break stop for opening hours

        Returns:
            str: Stored value of launch break stop, value like '12:30'
        """
        return self.__lunch_break_stop

    @lunch_break_stop.setter
    def lunch_break_stop(self, value: str):
        """Set lunch break stop for opening hours

        Args:
            data (str): Store value of launch break stop with value like '12:30'
        """
        self.__lunch_break_stop = value

    def _is_valid_time(self, time_value):
        """Check if a time value is valid (not None, not NaN, not 'nan' string).

        Args:
            time_value: The time value to validate

        Returns:
            bool: True if the value is a valid time string, False otherwise
        """
        if time_value is None:
            return False

        time_str = str(time_value).strip().lower()

        # Check for NaN variations
        if time_str in ('nan', 'none', ''):
            return False

        # Check if it looks like a valid time format (HH:MM:SS or HH:MM)
        if ':' not in time_str:
            return False

        return True

    def process(self):
        """Build the OSM opening_hours string from the per-day table set up in
        __init__: groups consecutive weekdays with identical open/close times into
        day ranges (e.g. 'Mo-Fr'), lists non-consecutive matches comma-separated,
        inserts the lunch-break split if lunch_break_start/stop are set, and appends
        the public-holiday suffix.

        Returns:
            str | None: '24/7' (+ PH suffix) if nonstop is True or every day resolves
            to Mo-Su 00:00-24:00; the built opening_hours string; or None if no day
            had both an open and a close time set.
        """
        oh = ''
        oh_list = []
        for row in self.df_dup.itertuples():
            if row.open is not None and row.close is not None:
                # Validate that times are not NaN or invalid
                if not self._is_valid_time(row.open) or not self._is_valid_time(row.close):
                    continue

                # Order by week days
                ordered = collections.OrderedDict(sorted(row.same.items(), key=lambda x: x[0]))
                same = list(ordered.values())
                # Public Holidays
                if self.__public_holiday_open is None:
                    oh_ph = ''
                elif self.__public_holiday_open is True:
                    oh_ph = '; PH open'
                elif self.__public_holiday_open is False:
                    oh_ph = '; PH off'
                else:
                    oh_ph = ''
                # Try to merge days interval
                if len(ordered) >= 2:
                    same_id = list(ordered.keys())
                    diffs = [same_id[i + 1] - same_id[i] for i in range(len(same_id) - 1)]
                    # Diffs list contains only 1 to make day interval
                    if diffs.count(1) == len(diffs):
                        days = '{}-{}'.format(list(ordered.values())[0], list(ordered.values())[-1])
                    # Make list of days
                    else:
                        days = ','.join(same)
                # Make list of days
                else:
                    days = ','.join(same)

                # Validate lunch break times if they exist
                lunch_break_valid = True
                if self.__lunch_break_start is not None or self.__lunch_break_stop is not None:
                    if not self._is_valid_time(self.__lunch_break_start) or not self._is_valid_time(self.__lunch_break_stop):
                        lunch_break_valid = False

                if lunch_break_valid:
                    if self.__lunch_break_start is None and self.__lunch_break_stop is None:
                        # If open and close are equals we handles as closed
                        if row.open != row.close:
                            oh_list.append(
                                "{} {}-{}".format(days.title(), row.open, row.close)
                            )
                    else:
                        # If open and close are equals we handles as closed
                        if row.open != row.close:
                            oh_list.append(
                                "{} {}-{},{}-{}".format(
                                    days.title(),
                                    row.open,
                                    self.__lunch_break_start,
                                    self.__lunch_break_stop,
                                    row.close,
                                )
                            )
                else:
                    # Lunch break is invalid, just use open-close
                    if row.open != row.close:
                        oh_list.append(
                            "{} {}-{}".format(days.title(), row.open, row.close)
                        )

                oh = '; '.join(oh_list)
                oh = oh + oh_ph
        if self.__non_stop is True or 'Mo-Su 00:00-24:00' in oh:
            try:
                return '24/7{}'.format(oh_ph)
            except Exception:
                return '24/7'
        elif oh_list == []:
            return None
        else:
            return oh
