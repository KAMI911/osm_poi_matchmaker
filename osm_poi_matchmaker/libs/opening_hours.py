# -*- coding: utf-8 -*-

try:
    import logging
    import pandas as pd
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)

class OpeningHours(object):

    def __init__(self, non_stop, mo_o, tu_o, we_o, th_o, fr_o, sa_o, su_o, mo_c, tu_c, we_c, th_c,
                          fr_c, sa_c, su_c, summer_mo_o, summer_tu_o, summer_we_o, summer_th_o, summer_fr_o,
                          summer_sa_o, summer_su_o, summer_mo_c, summer_tu_c, summer_we_c, summer_th_c,
                          summer_fr_c, summer_sa_c, summer_su_c, lunch_break_start, lunch_break_stop):
        self.non_stop = non_stop
        self.opening_hours = {'mo': [mo_o, mo_c, summer_mo_o, summer_mo_c, 0],
                              'tu': [tu_o, tu_c, summer_tu_o, summer_tu_c, 1],
                              'we': [we_o, we_c, summer_we_o, summer_we_c, 2],
                              'th': [th_o, th_c, summer_th_o, summer_fr_c, 3],
                              'fr': [fr_o, fr_c, summer_fr_o, summer_fr_c, 4],
                              'sa': [sa_o, sa_c, summer_sa_o, summer_sa_c, 5],
                              'su': [su_o, su_c, summer_su_o, summer_su_c, 6]}
        self.week_days = ( 'mo', 'tu', 'we', 'th', 'fr', 'sa', 'su')
        self.oh_types = ('open', 'close', 'summer_open', 'summer_close', 'did')
        self.df_oh = pd.DataFrame.from_dict(self.opening_hours, orient = 'index', columns=self.oh_types)
        self.df_dup = self.df_oh.sort_values('did').drop_duplicates(['open', 'close'], keep='first')
        self.df_dup['same'] = None
        nodes= [1,2,3,4]
        for k, v in self.df_dup.iterrows():
            self.df_dup.at[k, 'same'] = self.df_oh.loc[(self.df_oh['open'] == v['open']) & (self.df_oh['close'] == v['close'])].index.tolist()


    @property
    def nonstop(self):
        return (self.non_stop)

    @nonstop.setter
    def nonstop(self, value):
        self.nonstop = value

    @property
    def lunch_break(self):
        return (self.lunch_break)

    @lunch_break.setter
    def lunch_break(self, lunch_break_start, lunch_break_stop):
        self.lunch_break = { 'start': self.lunck_break_start, 'stop': self.lunch_break_stop}

    def process(self):
        oh = ''
        oh_list = []
        if self.non_stop == True:
            return '24/7'
        for k, v in self.df_dup.iterrows():
            if v['open'] is not None and v['close'] is not None:
                days = ','.join(sorted(v['same']))
                oh_list.append('{} {}-{}'.format(days.title(), self.df_dup.at[k, 'open'], self.df_dup.at[k, 'close']))
                oh = ';'.join(oh_list)
        if oh_list == []:
            oh = None
        return oh
