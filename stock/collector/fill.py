# -*- coding:utf-8 -*-
import uuid
import numpy as np
import pandas as pd
import schedule
import re
import datetime as dt
import psycopg.schema as schema
from threading import Thread
from stock.collector.basic import *
from stock.collector.detail import *
from Logger.Logger import Logger
from utils.Timer import LogTime


class Filler(Thread):
    def __init__(self, name, **kwargs):
        Thread.__init__(self, group=None, target=None, name=name,
                        args=(), kwargs=None, daemon=False)

        self.log = Logger('lib')
        self.basic = schema.get_schema('BASIC_STOCKS')
        self._meta = schema.get_schema('META_CODE')
        self.detail = schema.get_schema('DETAIL_MODEL')
        # Collect basic data from current records in db
        con = self.basic.get_con()
        self.meta = pd.read_sql_query('SELECT * FROM meta.code', con)
        # self.hs = pd.DataFrame()
        self.hs = kwargs['hs']


    def basic_daily(self):
        """
        Just update the stocks already in database, for the new stocks will process after it return
        """
        try:
            # self.log.info('Fetch stocks basic data from Tushare.')
            # global hs
            # self.hs = get_basics()

            # Add guid(uuid) column to HS DataFrame
            self.hs['guid'] = [uuid.uuid4() for _ in range(len(self.hs))]
            self.log.info('Update stocks basic in database.')
            self.update_basic(self.hs, self.meta)
            update = self.hs.reindex(['guid', 'code'], axis=1)
            # Filt new stock and create table for them
            add = update[[not item for item in self.hs['code'].isin(self.meta['code'])]]

            self.update_detail(self.hs[[bool(item) for item in self.hs['code'].isin(self.meta['code'])]])
            self.basic.commit()
            return add
        except Exception as err:
            self.basic.rollback()
            raise err


    def update_basic(self, df, meta_df):
        # Get current records from db, return 'giuid' and 'code'
        # select = basic.select(['guid', 'code'])

        # Update or insert BASIC_STOCKS
        for key, item in df.to_dict('index').items():
            if np.isin(item['code'], meta_df['code']):
                self.log.trace('%s already in basic table, update only.'%item['code'])
                guid = meta_df[meta_df.code==item['code']].guid.values[0]
                self.basic.update(guid, **item).execute()
            else:
                self.log.trace('Insert new stock into database: %s'%item['code'])
                self.basic.insert(**item).execute()


    def update_meta(self, guid=None, code=None):
        if guid is None and code is None:
            self.log.info('Flush META schema before insert records.')
            self._meta.delete('true').execute()
        else:
            self.log.info('Insert into META schema, code: %s'%code)
            self._meta.insert(guid=guid, code=code).execute()
        self._meta.commit()


    @LogTime()
    def update_detail(self, hs):
        self.log.info('Start update details, got %s to be updated.'%len(hs))
        _today = dt.date.today().isoformat()
        if not len(hs):
            return None
        for code in hs['code']:
            self.log.trace('Update stock detail for: %s'%code)
            # Test if stock detail already in DB, update or create the detail table
            res = self.detail.exist('s'+code).execute()
            if not res.fetch()[0][0]:
                self.log.info('Update stock %s not exist in DB, to create detail method.'%code)
                self.create_detail(hs[hs['code']==code])
            else:
                _d = get_detail(code, _today)
                if not _d.empty:
                    self.log.trace('Successfully get stock %s data for today: %s'%(code, _today))
                    __d = dict(list(_d.to_dict('index').values())[0])
                    self.detail.set_table('s_'+code).insert(**__d).execute()
                    guid = self.meta[self.meta.code==code].guid.values[0]
                    self.update_meta(guid=guid, code=code)
                    self.detail.commit()


    @LogTime()
    def _fetch_detail(self, code):
        self.log.trace('Start fetch detail for code: %s'%code)
        # Create table if not exist
        _t_name = 's_' + code
        res = self.detail.exist(_t_name).execute()
        if not res.fetch()[0][0]:
            self.log.info('"%s" not exist in DB, create table...'%_t_name)
            self.detail.set_table(_t_name).create_table().execute()
        else:
            self.log.info('"%s" already exist in DB, drop data...'%_t_name)
            self.detail.set_table(_t_name).delete('true').execute()
        # Get history data
        self.log.info('Fetch detail for code: %s'%code)
        try:
            _df = get_detail(code)
        except Exception:
            # If exception caught, skip this stock and continue next
            self.detail.commit()
            return False
        self.log.trace('Successfully got detail data : %s'%len(_df))
        for k, v in _df.to_dict('index').items():
            self.detail.set_table(_t_name).insert(**(v)).execute()
        return True


    def create_detail(self, add):
        self.log.info('Create detail table for newer records from daily basics: %s'%len(add))
        for item in add.to_dict('index').values():
            res = self._fetch_detail(item['code'])
            if res:
                self.update_meta(guid=item['guid'], code=item['code'])
            self.detail.commit()

    def run(self):
        self.log.info('Start daily task to fetch basic info')
        add = self.basic_daily()
        self.create_detail(add)
        self.basic.close()
        self._meta.close()
        self.detail.close()
        self.log.info('Finished basic data fetch, total: [%s]'%len(self.hs))
        self.log.info('Got newly stocks: [%s]'%len(add))
        return "{'total': %d, 'new': %d }"%(len(self.hs), len(add))



def do():
    hs = get_basics()
    total = len(hs.index)
    total = len(hs)    
    step = total//10
    for i in range(10):
        if i == 9:
            sub = hs[(step*i) : total]
        else:
            sub = hs[(step*i) : step*(i+1)]
        Filler('filler-%s'%i, hs=sub).start()


# hs['sh50'] = np.where(hs['code'].isin(sh['code']), True, False)

# hs.merge(hs300_, on='code', how='left')
# hs.rename({'weight': 'hs300'}, axis='columns', inplace=True)

# hs['zz500'] = np.where(hs['code'].isin(zz500['code']), True, False)

# Filte sme 中小板
# np.where(hs['code'].apply(lambda item: re.match(item, r'^002.*')), True, False)
