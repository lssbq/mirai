# -*- coding:utf-8 -*-
"""
Collect all stocks in HS and pre-process some columns and index
Return Pandas DataFrame and ready to update db
"""
import numpy as np
import pandas as pd
import tushare as ts
import datetime as _dt
import re
from urllib.error import HTTPError
from Error.NoData import NoData
import psycopg.pg_opt as pg
import psycopg.schema as schema
from Logger.Logger import Logger

log = Logger('lib')


def __get_hs(date=None) -> pd.DataFrame:
    """获取沪深所有股票数据: 
        返回DataFrame 默认code作为index，转换为普通column
    """
    if date is None:
        date = _dt.date.today().isoformat()
    elif isinstance(date, str):
        if not re.match(r'\d{4}-\d{2}-\d{2}', date):
            # TODO Log the error and use today
            date = _dt.date.today().isoformat()
            print('Format error or time exceed, use today')
    elif isinstance(date, _dt.date):
        if date > _dt.date.today():
            date = _dt.date.today().isoformat()
        else:
            date = date.isoformat()

    # 若返回404 NotFound 表示当天非交易日或者早于开市时间,使用上一交易日数据
    try:
        return ts.get_stock_basics(date).reset_index()
    except HTTPError as err:
        if err.getcode() == 404:
            raise NoData(date, 'Not Found')
        else:
            raise err
    except Exception as e:
            log.error('Could not fetch HS basics data at %s' % date)
            log.error(e)
            raise e

def __get_sh50() -> pd.DataFrame:
    """获取 上证50 成分股, 最新日期"""
    try:
        return ts.get_sz50s().reindex(['code', 'name', 'date'], axis= 1)
    except Exception as err:
        log.error("Could not fetch SH50 data")
        log.error(err)        
        raise err

def __get_hs300() -> pd.DataFrame:
    """获取 沪深300 成分股及权重weight, 最新日期"""
    try:
        hs = ts.get_hs300s().reindex(['code', 'weight'], axis= 1)
        hs.rename({'weight': 'hs300'}, axis='columns', inplace=True)
        return hs
    except Exception as err:
        log.error("Could not fetch HS300 data")
        log.error(err)  
        raise err

def __get_zz500() -> pd.DataFrame:
    """获取 中证500 成分股及权重weight, 最新日期"""
    try:
        return ts.get_zz500s().reindex(['code', 'name', 'date', 'weight'], axis= 1)
    except Exception as err:
        log.error("Could not fetch ZZ500 data")
        log.error(err)  
        raise err

def __filter_sme(hs):
    """过滤 中小板 成份股"""
    hs['sme'] = np.where(hs['code'].apply(lambda item: re.match(r'^002.*', str(item))), True, False)
    hs.fillna({'sme':False}, axis=0, inplace=True)

def __filter_gem(hs):
    """过滤 创业板 成份股"""
    hs['gem'] = np.where(hs['code'].apply(lambda item: re.match(r'^300.*', str(item))), True, False)

def __filter_st(hs):
    """过滤 ST 股票,风险警示股"""
    # TODO Chinese charachters matched by RegEx
    hs['st'] = np.where(hs['name'].apply(lambda item: item.find('ST')!=-1), True, False)

def __concept_classified(hs):
    """增加概念分类"""
    clazz = ts.get_concept_classified()
    clazz = clazz.groupby('name').agg({'code':'first','c_name':','.join})
    clazz.rename({'c_name':'concept'}, axis=1, inplace=True)
    clazz = clazz.reset_index(drop=True).reindex(['code','concept'], axis=1)
    hs = hs.merge(clazz, on='code', how='left')
    hs.fillna({'concept':''}, axis=0, inplace=True)
    return hs

# def __fill_industry(hs) -> None:
#     """获取行业分类,填充 industry 列"""
#     industry = ts.get_industry_classified(standard='sw')
#     industry = industry.reindex(['code','c_name'], axis=1)
#     hs = hs.merge(industry, on='code', how='left')


def __get_hs_daily(date):
    try:
        return __get_hs(date)
    except NoData as err:
        yestoday = err.getDatetime() - _dt.timedelta(days=1)
        log.error('Stocks data on %s not found. Get %s data.' %(date, yestoday))
        return __get_hs_daily(yestoday)



def get_basics():
    """
    Get HS stocks basics for the latest exchange day and return DataFrame to update db
    """
    today = _dt.date.today().isoformat()
    log.info('Start to collect HS_basics_all stocks...')
    log.info('Collecting the main stock basics from tushare for %s' % today)
    hs = __get_hs_daily(today)
    
    log.info('Collecting SH50 and prepare to merge into the main frame...')
    sh50 = __get_sh50()
    log.info('Collection HS300 and prepare to merge into the main frame...')
    hs300 = __get_hs300()
    log.info('Collection ZZ500 and prepare to merge into the main frame...')    
    zz500 = __get_zz500()

    # Add 上证50 column
    log.trace('Adding SH50 to main frame as \'sh50\'')
    hs['sh50'] = np.where(hs['code'].isin(sh50['code']), True, False)
    # Merge from hs300 and change column name to 'hs300'
    log.trace('Adding HS300 to main frame as \'hs300\'')    
    hs = hs.merge(hs300, on='code', how='left')
    hs.fillna({'hs300':0}, axis=0, inplace=True)
    # Add 中证500 column
    log.trace('Adding ZZ500 to main frame as \'zz500\'')
    hs['zz500'] = np.where(hs['code'].isin(zz500['code']), True, False)
    # Filte 中小板
    log.trace('Adding SME to main frame as \'sme\'')
    __filter_sme(hs)
    # Filte 创业板
    log.trace('Adding GEM to main frame as \'gem\'')
    __filter_gem(hs)
    # Filte ST
    log.trace('Filting ST and tag to main frame as \'st\'')
    __filter_st(hs)
    # Fill 概念分类
    log.trace('Adding concept classfied \'concept\'')
    hs = __concept_classified(hs)

    # Reset index, prepare to write to database
    log.info('Reindexing the DataFrame and rename to match the column name in db...')
    hs = hs.reindex(['code','name','industry','concept','sme','gem','st','hs300','sh50','zz500','pe','outstanding','totals','totalAssets','liquidAssets','timeToMarket','pb','rev','profit','holders'], axis=1)
    hs.rename({'totalAssets': 'total_assets', 'liquidAssets': 'liquid_assets', 'timeToMarket': 'time_to_market'}, axis='columns', inplace=True)

    log.info('Successfully collect stock basics, got [%s] records.' % len(hs))
    
    return hs


if __name__ == '__main__':
    print('Load complated!')
    print(get_basics())