# -*- coding:utf-8 -*-
import re
import numpy as np
import pandas as pd
import datetime as dt
from psycopg import schema

basic_schema = schema.get_schema('BASIC_STOCKS')
con = basic_schema.get_con()

#  Filter conditions:
#   SH(600*), SZ(00*), gem, sh50, hs300, zz500, sme, st
#   industry
#   total_assets
#   time_to_market
#   pe
class Pool():
    def __init__(self, **conditions):
        assert not con.closed, 'DB connection not avaliable now.'
        _stock_frame = pd.read_sql_query('SELECT * FROM basic.stock_basic ORDER BY code;', con)
        assert not _stock_frame.empty, 'Cannot load stocks from DB or is empty.'
        self.stocks = _stock_frame
        self.filter = {'SH': self._SH, 'SZ': self._SZ, 'gem': self._gem, 'sh50': self._sh50, 'hs300': self._hs300,
                        'zz500': self._zz500, 'sme': self._sme, 'st': self._st, 'industry': self._industry, 'pe': self._pe,
                        'total_assets': self._total_assets, 'time_to_market': self._ttm, 'concept': self._concept}

        if bool(conditions):
            for k,v in conditions.items():
                _f = self.filter.get(k, None)
                if _f is not None and callable(_f):
                    _f(v)
            self.stocks.reset_index(inplace=True, drop=True)

    def _SH(self, _flag):
        self.stocks = self.stocks[self.stocks['code'].apply(lambda i: bool(re.match(r'^600.*',i)) == bool(_flag))]

    def _SZ(self, _flag):
        self.stocks = self.stocks[self.stocks['code'].apply(lambda i: bool(re.match(r'^00.*',i)) == bool(_flag))]

    def _gem(self, _flag):
        self.stocks = self.stocks[self.stocks['gem'] == bool(_flag)]

    def _sh50(self, _flag):
        self.stocks = self.stocks[self.stocks['sh50'] == bool(_flag)]

    def _hs300(self, _flag):
        self.stocks = self.stocks[self.stocks['hs300'] == bool(_flag)]

    def _zz500(self, _flag):
        self.stocks = self.stocks[self.stocks['zz500'] == bool(_flag)]

    def _sme(self, _flag):
        self.stocks = self.stocks[self.stocks['sme'] == bool(_flag)]

    def _st(self, _flag):
        self.stocks = self.stocks[self.stocks['st'] == bool(_flag)]

    def _industry(self, arg):
        pattern = arg.get('pattern')
        flag = bool(arg.get('flag', True))
        pattern = r'.*' + pattern + r'.*'
        self.stocks = self.stocks[self.stocks['industry'].apply(lambda i: bool(re.match(pattern,i)) == flag)]
    
    def _concept(self, arg):
        pattern = arg.get('pattern')
        flag = bool(arg.get('flag', True))
        pattern = r'.*' + pattern + r'.*'
        self.stocks = self.stocks[self.stocks['concept'].apply(lambda i: bool(re.match(pattern,i)) == flag)]

    # Assets: ---------| low |-----| high |---------
    # total_assets < low
    # low < total_assets < high
    # total_assets > high
    def _total_assets(self, arg):
        low = arg.get('low')
        high = arg.get('high')
        flag = bool(arg.get('flag', True))
        condition = lambda i: True
        if low is None:
            condition = lambda i: bool(i > high) == flag
        elif high is None:
            condition = lambda i: bool(i < low) == flag
        else:
            condition = lambda i: bool(i > low and i < high) == flag

        self.stocks = self.stocks[self.stocks['total_assets'].apply(condition)]

    # Time line: ---------| start |-----| end |---------
    # time_to_market < start
    # start < time_to_market < end
    # time_to_market > end
    def _ttm(self, arg):
        start = arg.get('start')
        end = arg.get('end')
        flag = bool(arg.get('flag', True))
        start_t = None
        end_t = None
        condition = lambda i: True
        if start is None:
            end_t = dt.datetime.strptime(end,'%Y-%m-%d')
            condition = lambda i: bool(i > dt.datetime.date(end_t)) == flag
        elif end is None:
            start_t = dt.datetime.strptime(start,'%Y-%m-%d')
            condition = lambda i: bool(i < dt.datetime.date(start_t)) == flag
        else:
            start_t = dt.datetime.strptime(start,'%Y-%m-%d')
            end_t = dt.datetime.strptime(end,'%Y-%m-%d')
            condition = lambda i: bool(i > dt.datetime.date(start_t) and dt.datetime.date(i < end_t)) == flag
        self.stocks = self.stocks[self.stocks['time_to_market'].apply(condition)]

    # PE: ---------| low |-----| high |---------
    # pe < low
    # low < pe < high
    # pe > high
    def _pe(self, arg):
        low = arg.get('low')
        high = arg.get('high')
        flag = bool(arg.get('flag', True))
        condition = None
        if low is None:
            condition = lambda i: bool(i > high) == flag
        elif high is None:
            condition = lambda i: bool(i < low) == flag
        else:
            condition = lambda i: bool(i > low and i < high) == flag
        self.stocks = self.stocks[self.stocks['pe'].apply(condition)]

    def apply(self, cb):
        assert callable(cb), 'apply() must called with a function.'

