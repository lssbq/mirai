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

log = Logger('lib')


# schedule.every().day.at("15:10").do(job)
# while True:
#     schedule.run_pending()
db = pg.DB().get_exec()
con = db.get_con()


# Collect basic data from current records in db
meta = pd.read_sql_query('SELECT * FROM meta.code', con)


def basic_daily():
    try:
        hs = get_basics()
        # Add guid(uuid) column to HS DataFrame
        hs['guid'] = [uuid.uuid4() for _ in range(len(hs))]

        update_basic(hs, meta)
        update = hs.reindex(['guid', 'code'], axis=1)
        add = update[[not item for item in hs['code'].isin(meta['code'])]]
        update_meta(update)
        update_detail(hs[[bool(item) for item in hs['code'].isin(meta['code'])]])
        db.commit()
        return add
    except Exception as err:
        db.rollback()
        raise err


def update_basic(df, meta_df):
    # Get current records from db, return 'giuid' and 'code'
    # select = schema.get_schema('BASIC_STOCKS').select(['guid', 'code'])

    # Update or insert record
    for key, item in df.to_dict('index').items():
        if np.isin(item['code'], meta_df['code']):
            schema.get_schema('BASIC_STOCKS').update(meta_df[meta_df.code==item['code']].guid.values[0], **item).execute()
        else:
            schema.get_schema('BASIC_STOCKS').insert(**item).execute()


def update_meta(meta):
    schema.get_schema('META_CODE').delete('true').execute()

    for key, item in meta.to_dict('index').items():
        schema.get_schema('META_CODE').insert(**item).execute()


def update_detail(hs):
    log.info('Start update details, got %s to be updated.'%len(hs))
    _today = dt.date.today().isoformat()
    if not len(hs):
        return None
    for code in hs['code']:
        log.trace('Update stock detail for: %s'%code)
        _d = get_detail(code, _today)
        if not _d.empty:
            log.trace('Successfully get stock %s data for today: %s'%(code, _today))
            schema.get_schema('DETAIL_MODEL').set_table('s_'+code).insert(**(_d.to_dict('index').items())).execute()


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
        schema.get_schema('DETAIL_MODEL').set_table(_t_name).delete().execute()
    # Get history data
    log.info('Fetch detail for code: %s'%code)
    _df = get_detail(code)
    log.trace('Successfully got detail data : %s'%len(_df))
    for k, v in _df.to_dict('index').items():
        schema.get_schema('DETAIL_MODEL').set_table(_t_name).insert(**(v)).execute()
    

def create_detail(add):
    log.info('Create detail table for newer records from daily basics: %s'%len(add))
    for item in add.to_dict('index').values():
        _fetch_detail(item['code'])



if __name__ == '__main__':
    add = basic_daily()
    create_detail(add)
    con.commit()
    
    



# hs['sh50'] = np.where(hs['code'].isin(sh['code']), True, False)

# hs.merge(hs300_, on='code', how='left')
# hs.rename({'weight': 'hs300'}, axis='columns', inplace=True)

# hs['zz500'] = np.where(hs['code'].isin(zz500['code']), True, False)

# Filte sme 中小板
# np.where(hs['code'].apply(lambda item: re.match(item, r'^002.*')), True, False)