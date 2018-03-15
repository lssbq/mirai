# -*- coding:utf-8 -*-
import uuid
import numpy as np
import pandas as pd
import schedule
import re
import datetime as dt
import psycopg.pg_opt as pg
import psycopg.schema as schema
from stock.collector.basic import *
from stock.collector.detail import *
from Logger.Logger import Logger
from utils.Timer import LogTime

log = Logger('lib')

db = pg.DB().get_exec()
con = db.get_con()


# Collect basic data from current records in db
meta = pd.read_sql_query('SELECT * FROM meta.code', con)
hs = pd.DataFrame()


def basic_daily():
    """
    Just update the stocks already in database, for the new stocks will process after it return
    """
    try:
        log.info('Fetch stocks basic data from Tushare.')
        global hs
        hs = get_basics()
        # Add guid(uuid) column to HS DataFrame
        hs['guid'] = [uuid.uuid4() for _ in range(len(hs))]
        log.info('Update stocks basic in database.')
        update_basic(hs, meta)
        update = hs.reindex(['guid', 'code'], axis=1)
        # Filt new stock and create table for them
        add = update[[not item for item in hs['code'].isin(meta['code'])]]
        log.info('Overwrite stocks base info(guid,code) in META schema.')
        update_meta()
        update_detail(hs[[bool(item) for item in hs['code'].isin(meta['code'])]])
        db.commit()
        return add
    except Exception as err:
        db.rollback()
        raise err


def update_basic(df, meta_df):
    # Get current records from db, return 'giuid' and 'code'
    # select = schema.get_schema('BASIC_STOCKS').select(['guid', 'code'])

    # Update or insert BASIC_STOCKS
    for key, item in df.to_dict('index').items():
        if np.isin(item['code'], meta_df['code']):
            log.trace('%s already in basic table, update only.'%item['code'])
            guid = meta_df[meta_df.code==item['code']].guid.values[0]
            schema.get_schema('BASIC_STOCKS').update(guid, **item).execute()
        else:
            log.trace('Insert new stock into database: %s'%item['code'])
            schema.get_schema('BASIC_STOCKS').insert(**item).execute()


def update_meta(guid=None, code=None):
    if guid is None and code is None:
        log.info('Flush META schema before insert records.')
        schema.get_schema('META_CODE').delete('true').execute()
    else:
        log.info('Insert into META schema, code: %s'%code)
        schema.get_schema('META_CODE').insert(guid=guid, code=code).execute()


@LogTime(logger=log)
def update_detail(hs):
    log.info('Start update details, got %s to be updated.'%len(hs))
    _today = dt.date.today().isoformat()
    if not len(hs):
        return None
    for code in hs['code']:
        log.trace('Update stock detail for: %s'%code)
        # Test if stock detail already in DB, update or create the detail table
        schema.get_schema('DETAIL_MODEL').exist('s'+code).execute()
        if not db.fetch()[0][0]:
            log.info('Update stock %s not exist in DB, to create detail method.'%code)
            create_detail(hs[hs['code']==code])
        else:
            _d = get_detail(code, _today)
            if not _d.empty:
                log.trace('Successfully get stock %s data for today: %s'%(code, _today))
                __d = dict(list(_d.to_dict('index').values())[0])
                schema.get_schema('DETAIL_MODEL').set_table('s_'+code).insert(**__d).execute()
                guid = meta[meta.code==code].guid.values[0]
                update_meta(guid=guid, code=code)
                db.commit()


@LogTime(logger=log)
def _fetch_detail(code):
    log.trace('Start fetch detail for code: %s'%code)
    # Create table if not exist
    _t_name = 's_' + code
    schema.get_schema('DETAIL_MODEL').exist(_t_name).execute()
    if not db.fetch()[0][0]:
        log.info('"%s" not exist in DB, create table...'%_t_name)
        schema.get_schema('DETAIL_MODEL').set_table(_t_name).create_table().execute()
    else:
        log.info('"%s" already exist in DB, drop data...'%_t_name)
        schema.get_schema('DETAIL_MODEL').set_table(_t_name).delete('true').execute()
    # Get history data
    log.info('Fetch detail for code: %s'%code)
    try:
        _df = get_detail(code)
    except Exception:
        # If exception caught, skip this stock and continue next
        db.commit()
        return False
    log.trace('Successfully got detail data : %s'%len(_df))
    for k, v in _df.to_dict('index').items():
        schema.get_schema('DETAIL_MODEL').set_table(_t_name).insert(**(v)).execute()
    return True


def create_detail(add):
    log.info('Create detail table for newer records from daily basics: %s'%len(add))
    for item in add.to_dict('index').values():
        res = _fetch_detail(item['code'])
        if res:
            update_meta(guid=item['guid'], code=item['code'])
        db.commit()


def do():
    log.info('Start daily task to fetch basic info')
    add = basic_daily()
    create_detail(add)
    log.info('Finished basic data fetch, total: [%s]'%len(hs))
    log.info('Got newly stocks: [%s]'%len(add))
    return "{'total': %d, 'new': %d }"%(len(hs), len(add))





# hs['sh50'] = np.where(hs['code'].isin(sh['code']), True, False)

# hs.merge(hs300_, on='code', how='left')
# hs.rename({'weight': 'hs300'}, axis='columns', inplace=True)

# hs['zz500'] = np.where(hs['code'].isin(zz500['code']), True, False)

# Filte sme 中小板
# np.where(hs['code'].apply(lambda item: re.match(item, r'^002.*')), True, False)