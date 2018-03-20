# -*- coding:utf-8 -*-
"""
Collect index from HS market.
    code: 代码
    name: 名称
    change: 涨跌幅
    open: 开盘
    preclose: 昨收
    close: 收盘
    high: 高点
    low: 低点
    volume: 成交量
    amount: 成交额
"""

import uuid
import numpy as np
import pandas as pd
import tushare as ts
import datetime as dt
import psycopg.schema as schema
from urllib.error import HTTPError
from Logger.Logger import Logger
from utils.Timer import LogTime

INDEX = ['000001', '000002', '000003', '000008', '000009', '000010', '000011', '000012', '000016', '000017', '000300', '000905', '399001', '399002', '399003', '399004', '399005', '399006', '399008', '399100', '399101', '399106', '399107', '399108', '399333', '399606']
OLD = {'000001': 'sh', '399001': 'sz', '000300': 'hs300', '000016': 'sz50', '399005': 'zxb', '399006': 'cyb'}


log = Logger('lib')

_index = schema.get_schema('INDEX')
meta_index = schema.get_schema('META_INDEX')
detail = schema.get_schema('DETAIL_MODEL')


# Collect basic data from current records in db
con = _index.get_con()
meta = pd.read_sql_query('SELECT * FROM meta.index', con)


def get_index():
    _today = dt.date.today().isoformat()

    log.info('Start update index on : %s'%_today)

    index = ts.get_index()
    # Create table for new index data which code not in the meta table
    create_table(index[np.isin(index['code'], meta['code'], invert=True)])
    index['guid'] = [uuid.uuid4() for _ in index['code']]

    for code in index['code']:
        if code in list(meta['code']):
            # Update only
            log.info('Update index %s on %s'%(code, _today))
            get_index_detail(code, _today)
            # Only update specific values, need to reindex
            _v = list(index[index['code']==code]
                .reindex(['change','open','close','high','low','preclose','volume','amount'], axis=1)
                .to_dict('index').values())[0]
            _index.update(meta[np.isin(meta['code'], code)]['guid'].iloc[0], **_v).execute()
        else:
            # Fetch all for new index not in meta
            log.info('New index code %s, fetch all data.'%code)
            get_index_detail(code)
            _v = list(index[index['code']==code].to_dict('index').values())[0]
            _index.insert(**_v).execute()
    
    # Update metadata
    meta_index.delete('True').execute()
    log.info('Update META for index')

    for item in index.reindex(['guid', 'code'], axis=1).to_dict('index').values():
        meta_index.insert(**item).execute()

    meta_index.commit()
    _index.commit()

    return len(index)


def create_table(code):
    if code.empty:
        return None
    for item in code['code']:
        log.info('Create table for new index: %s'%item)
        detail.set_table('i_'+item).create_table().execute()
    # Should be commit immediately after create table
    detail.commit()


@LogTime(logger=log)
def get_index_detail(code, date=None):
    """ Get or Update index detail """
    _old = OLD.keys()
    if code in INDEX and code in _old:
        log.info('Fetch index detail with OLD API for : %s'%code)
        data = __get_hist(OLD[code], date=date)
    else:
        data = __get_k(code, date=date)
        log.info('Fetch index detail with NEW API for : %s'%code)
    # If data is empty DataFrame, for loop will not execute
    log.trace('Insert [%s] data into i_%s'%(len(data.index), code))
    for item in data.to_dict('index').values():
        # Insert all data for new index or insert today data to update
        detail.set_table('i_'+code).insert(**item).execute()
    detail.commit()


@LogTime(logger=log)
def __get_hist(code, date=None):
    """ 获取沪深指数Tushare Old API """
    log.info('Get history data from old API for code: %s'%code)
    try:
        if date is None:
            _data = ts.get_hist_data(code)
        else:
            _data = ts.get_hist_data(code, start=date)
        if _data is not None and not _data.empty:
            return _data.reset_index()
        else:
            return pd.DataFrame()
    except Exception as err:
        log.error('OLD API: Could not fetch index details.')
        raise err


@LogTime(logger=log)
def __get_k(code, date=None):
    """ 获取沪深重要指数 Tushare New API"""
    log.info('Get k data from new API for code: %s'%code)
    try:
        if date is None:
            _data = ts.get_k_data(code, index=True)
        else:
            _data = ts.get_k_data(code, index=True, start=date)
        if _data is not None and not _data.empty:
            return _data.reindex(['date', 'open', 'close', 'high', 'low', 'volume'], axis=1)
        else:
            return pd.DataFrame()
    except Exception as err:
        log.error('New API: Could not fetch index details.')
        raise err



def do():
    log.info('Start daily task to fetch index info')
    length = get_index()
    _index.close()
    meta_index.close()
    detail.close()
    log.info('Finished index data fetch, total: [%s]'%length)
    return "{'index': %d }"%length


if __name__ == '__main__':
    get_index()