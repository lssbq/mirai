# -*- coding:utf-8 -*-
"""
Collect details for single stock
"""
import numpy as np
import pandas as pd
import tushare as ts
from urllib.error import HTTPError
from Logger.Logger import Logger

log = Logger('lib')

def get_detail_old(code, date=None):
    """ Get 3 years data """
    try:
        log.trace("Fetch history data form old API(get_hist_data): %s"%code)
        if date == None:
            _data = ts.get_hist_data(code, retry_count=6)
            if _data is not None:
                return _data.reset_index()
            else:
                return pd.DataFrame()
        else:
            _data = ts.get_hist_data(code=code, start=date, retry_count=6)
            if _data is not None:
                return _data.reset_index()
            else:
                return pd.DataFrame()
    except Exception as err:
        log.error("(OLD) Could not fetch details for: %s"%code)
        log.error('Caused by: %s'%str(err))
        raise err

# get_k_data() : The new API to fetch stock hist data
#   code, *ktype='D', *autype='qfq', *index=False, start, end
def get_detail_new(code, date=None, ttm=None):
    """ Get history data from new api, from time_to_market or today."""
    try:
        log.trace("Fetch history data from new API(get_k_data): %s"%code)
        if date == None: # Fetch all hist data, from time_to_market
            assert ttm is not None, 'Time to market must be specifiy.'
            _data = ts.get_k_data(code=code, start=ttm)
            if _data is not None:
                return __post_get(_data.reindex(['date','open','close','high','low','volume'], axis=1)
                                    .reset_index(drop=True))
            else:
                return pd.DataFrame()
        else:
            _data = ts.get_k_data(code=code, start=date, end=date)
            if _data is not None:
                return __post_get(_data.reindex(['date','open','close','high','low','volume'], axis=1)
                                       .reset_index(drop=True))
            else:
                return pd.DataFrame()
    except Exception as err:
        log.error("(NEW) Could not fetch details for: %s"%code)
        log.error('Caused by: %s'%str(err))
        raise err


def __post_get(df):
    """ Calculate price change, p change """
    price = list(df['close'])
    pre_close = 0
    def price_change(p):
        nonlocal pre_close
        _pc = (round(p-pre_close, 2) if pre_close>0 else 0)
        pre_close = p
        return _pc
    df['price_change'] = df['close'].apply(lambda x: price_change(x))
    p_close = 0
    def p_change(p):
        nonlocal p_close
        _pc = ((round(p-p_close,2)/p_close) if p_close>0 else 0)
        p_close = p
        return _pc
    df['p_change'] = df['close'].apply(lambda x: p_change(x))
    return df


def get_detail(code: str, *, date=None, ttm=None):
    try:
        log.trace('Fetch history data for single stock: \'%s\''%code)
        new = get_detail_new(code, date, ttm)
        old = get_detail_old(code, date)
        if old is not None and old.empty:
            new['turnover'] = -1
            return new
        if new is not None and new.empty:
            old = old.reindex(['date','open','close','high','low','volume','turnover'], axis=1, fill_value=-1)
            return old

        res = pd.merge(new, old.reindex(['date','turnover'], axis=1), on='date', how='left').fillna(-1)
        if date is not None:
            res.update(old.reindex(['price_change', 'p_change'], axis=1))

    except Exception as err:
        log.error("Could not fetch details for: %s"%code)
        log.error('Caused by: %s'%str(err))
        return pd.DataFrame()
    return res

__all__ = ['get_detail']