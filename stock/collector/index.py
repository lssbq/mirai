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
import psycopg.schema as schema
from urllib.error import HTTPError
from Logger.Logger import Logger
from utils.Timer import LogTime
from socket import timeout
from threading import Thread, Event

INDEX = ['000001', '000002', '000003', '000008', '000009', '000010', '000011', '000012', '000016', '000017', '000300', '000905', '399001', '399002', '399003', '399004', '399005', '399006', '399008', '399100', '399101', '399106', '399107', '399108', '399333', '399606']
OLD = {'000001': 'sh', '399001': 'sz', '000300': 'hs300', '000016': 'sz50', '399005': 'zxb', '399006': 'cyb'}


class Indexer(Thread):

    def __init__(self, name, date, q, event=None):
        Thread.__init__(self, group=None, target=None, name=name,
                        args=(), kwargs=None, daemon=False)
        
        self.ready = event
        self.log = Logger('lib')
        self._index = schema.get_schema('INDEX')
        self.meta_index = schema.get_schema('META_INDEX')
        self.detail = schema.get_schema('DETAIL_MODEL')
        # Collect basic data from current records in db
        con = self._index.get_con()
        self.meta = pd.read_sql_query('SELECT * FROM meta.index', con)
        self.q = q
        self.today = date


    def __get_index(self):
        try:
            return ts.get_index()
        except timeout:
            # Retry when timeout
            return ts.get_index()


    def get_index(self):
        _today = self.today

        self.log.info('Start update index on : %s'%_today)

        index = self.__get_index()
        # Create table for new index data which code not in the meta table
        self.create_table(index[np.isin(index['code'], self.meta['code'], invert=True)])
        index['guid'] = [uuid.uuid4() for _ in index['code']]

        for code in index['code']:
            if code in list(self.meta['code']):
                # Update only
                self.log.info('Update index %s on %s'%(code, _today))
                self.get_index_detail(code, _today)
                # Only update specific values, need to reindex
                _v = list(index[index['code']==code]
                    .reindex(['change','open','close','high','low','preclose','volume','amount'], axis=1)
                    .to_dict('index').values())[0]
                self._index.update(self.meta[np.isin(self.meta['code'], code)]['guid'].iloc[0], **_v).execute()
            else:
                # Fetch all for new index not in meta
                self.log.info('New index code %s, fetch all data.'%code)
                self.get_index_detail(code)
                _v = list(index[index['code']==code].to_dict('index').values())[0]
                self._index.insert(**_v).execute()
        
        # Update metadata
        self.meta_index.delete('True').execute()
        self.log.info('Update META for index')

        for item in index.reindex(['guid', 'code'], axis=1).to_dict('index').values():
            self.meta_index.insert(**item).execute()

        self.meta_index.commit()
        self._index.commit()

        return len(index)


    def create_table(self, code):
        if code.empty:
            return None
        for item in code['code']:
            self.log.info('Create table for new index: %s'%item)
            self.detail.set_table('i_'+item).create_table().execute()
        # Should be commit immediately after create table
        self.detail.commit()


    @LogTime()
    def get_index_detail(self, code, date=None):
        """ Get or Update index self.detail """
        _old = OLD.keys()
        if code in INDEX and code in _old:
            self.log.info('Fetch index self.detail with OLD API for : %s'%code)
            data = self.__get_hist(OLD[code], date=date)
        else:
            data = self.__get_k(code, date=date)
            self.log.info('Fetch index self.detail with NEW API for : %s'%code)
        # If data is empty DataFrame, for loop will not execute
        self.log.trace('Insert [%s] data into i_%s'%(len(data.index), code))
        for item in data.to_dict('index').values():
            # Insert all data for new index or insert today data to update
            self.detail.set_table('i_'+code).insert(**item).execute()
        self.detail.commit()


    @LogTime()
    def __get_hist(self, code, date=None):
        """ 获取沪深指数Tushare Old API """
        self.log.trace('Get history data from old API for code: %s'%code)
        try:
            if date is None:
                _data = ts.get_hist_data(code)
            else:
                _data = ts.get_hist_data(code, start=date)
            if _data is not None and not _data.empty:
                return _data.reset_index().reindex(['date','open','high','close','low','volume',
                                                    'price_change','p_change','turnover'], axis=1)
            else:
                return pd.DataFrame()
        except Exception as err:
            self.log.error('OLD API: Could not fetch index details.')
            raise err


    @LogTime()
    def __get_k(self, code, date=None):
        """ 获取沪深重要指数 Tushare New API"""
        self.log.trace('Get k data from new API for code: %s'%code)
        try:
            if date is None:
                _data = ts.get_k_data(code, index=True)
            else:
                _data = ts.get_k_data(code, index=True, start=date)
            if _data is not None and not _data.empty:
                # TODO Calculate price change and p change, ignore turnover
                return _data.reindex(['date', 'open', 'close', 'high', 'low', 'volume'], axis=1)
            else:
                return pd.DataFrame()
        except Exception as err:
            self.log.error('New API: Could not fetch index details.')
            raise err


    def run(self):
        self.log.info('Start daily task to fetch index info')
        length = self.get_index()
        self._index.close()
        self.meta_index.close()
        self.detail.close()
        self.log.info('Finished index data fetch, total: [%s]'%length)
        self.ready.set()
        self.q.put({'index':length})
        # return "{'index': %d }"%length


def do(today, q):
    ready = Event()
    Indexer(name='Index-1', date=today, q=q, event=ready).start()

    # ready.wait()

    return None
