# -*- coding:utf-8 -*-
import uuid
import pandas as pd
import datetime as dt
import psycopg.schema as schema
from threading import Thread
from stock.collector.basic import *
from stock.collector.detail import get_detail
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
        self.today = kwargs['today']
        self.hs = kwargs['hs']
        self.q = kwargs['q']


    def basic_daily(self):
        """
        Just update the stocks already in database, for the new stocks will process after it return
        """
        try:
            # Add guid(uuid) column to HS DataFrame
            self.hs['guid'] = [uuid.uuid4() for _ in range(len(self.hs))]
            self.log.info('Update stocks basic in database.')
            # Filt new stock and create table for them
            add = self.hs[[not item for item in self.hs['code'].isin(self.meta['code'])]]

            self.update_detail(self.hs[[bool(item) for item in self.hs['code'].isin(self.meta['code'])]])
            return add
        except Exception as err:
            self.basic.rollback()
            raise err


    def update_basic(self, guid, df, isnew = False):
        # Update or insert BASIC_STOCKS
        item = list(df.to_dict('index').values())[0]
        item['guid'] = guid
        if not isnew:
            self.log.trace('%s already in basic table, update only.'%item['code'])
            # guid = self.meta[self.meta.code==item['code']].guid.values[0]
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
        _today = self.today
        if not len(hs):
            return None
        for code in hs['code']:
            self.log.trace('Update stock detail for: \'%s\' on %s'%(code,_today))
            guid = self.meta[self.meta.code==code].guid.values[0]
            # Test if stock detail already in DB, update or create the detail table
            res = self.detail.set_table('s_'+code).exist()
            if not res:
                self.log.trace('Update stock %s not exist in DB, to create detail method.'%code)
                self.create_detail(hs[hs['code']==code], guid)
            else:
                _d = get_detail(code, date=_today)
                if not _d.empty:
                    self.log.trace('Successfully get stock %s data for today: %s'%(code, _today))
                    __d = dict(list(_d.to_dict('index').values())[0])
                    self.detail.set_table('s_'+code).insert(**__d).execute()
                    self.update_basic(guid, hs[hs['code']==code])
                    self.basic.commit()
                    self.detail.commit()


    @LogTime()
    def _fetch_detail(self, code):
        self.log.trace('Start fetch detail for code: %s'%code)
        # Create table if not exist
        _t_name = 's_' + code
        # Get history data
        try:
            ttm = self.hs[self.hs['code']==code]['time_to_market'].values[0]
            if ttm:
                ttm = dt.datetime.strptime(str(ttm),'%Y%m%d').strftime('%Y-%m-%d')
            _df = get_detail(code, ttm=ttm)
        except Exception:
            return False
        self.log.trace('Successfully got detail data : %s'%len(_df))
        # New stock returns empty dataframe, skip it.
        if _df.empty:
            return False
        # Prepare table in db
        res = self.detail.set_table(_t_name).exist()
        if not res:
            self.log.info('"%s" not exist in DB, create table...'%_t_name)
            self.detail.set_table(_t_name).create_table().execute()
        else:
            self.log.info('"%s" already exist in DB, drop data...'%_t_name)
            self.detail.set_table(_t_name).delete('true').execute()
        # Insert into detail table
        for k, v in _df.to_dict('index').items():
            self.detail.set_table(_t_name).insert(**(v)).execute()
        return True


    def create_detail(self, add, guid=None):
        self.log.info('Create detail for newer records from daily basics: %s'%len(add))
        for item in add.to_dict('index').values():
            res = self._fetch_detail(item['code'])
            if res:
                gid = guid if guid is not None else item['guid']
                self.update_basic(gid, item, True)
                self.update_meta(guid=item['guid'], code=item['code'])
            self.basic.commit()
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
        self.q.put({'stocks':len(self.hs) , 'new':len(add) })



def do(today, q, threads=10):
    hs = get_basics()
    total = len(hs.index)
    total = len(hs)    
    step = total//threads
    for i in range(threads):
        if i == threads-1:
            sub = hs[(step*i) : total]
        else:
            sub = hs[(step*i) : step*(i+1)]
        Filler('filler-%s'%i, today=today, hs=sub, q=q).start()


# hs['sh50'] = np.where(hs['code'].isin(sh['code']), True, False)

# hs.merge(hs300_, on='code', how='left')
# hs.rename({'weight': 'hs300'}, axis='columns', inplace=True)

# hs['zz500'] = np.where(hs['code'].isin(zz500['code']), True, False)

# Filte sme 中小板
# np.where(hs['code'].apply(lambda item: re.match(item, r'^002.*')), True, False)