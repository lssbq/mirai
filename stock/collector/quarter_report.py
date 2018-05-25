# -*- coding:utf-8 -*-
import uuid
import math
import copy
import numpy as np
import pandas as pd
import tushare as ts
import datetime as dt
import psycopg.schema as schema
from queue import Queue
from threading import Thread
from Logger.Logger import Logger
from stock.collector.basic import get_basics


logger = Logger('lib')
db_o = schema.get_schema('REPORT')

_today = dt.date.today()
YEAR = _today.year
QUARTER = math.ceil(_today.month / 3)


def get_profit(year, quarter, q):
    """
        eps: 每股收益
        eps_yoy: 每股收益同比
        bvps: 每股净资产
        roe: 净资产收益率
        epcf: 每股现金流
        net_profits: 净利润（万元）
        profits_yoy: 净利润同比
        net_profit_ratio: 净利率
        gross_profit_rate: 毛利率
        business_income: 营业收入（百万）
        bips: 每股主营业务收入
        report_data: 发布日期
    """
    report = ts.get_report_data(year, quarter).drop_duplicates() \
               .reindex(['code','eps','eps_yoy','bvps','roe','epcf','net_profits','profits_yoy','report_date'], axis=1)
    profit = ts.get_profit_data(year, quarter).drop_duplicates() \
               .reindex(['code','net_profit_ratio','gross_profit_rate','business_income','bips'], axis=1)
    res = report.merge(profit, on='code', how='outer').set_index(['code'], drop=True)
    q.put({'name': 'profit', 'data': res.to_dict('index')})
    logger.info('Fetch data \'profit\' for %s-%s ... Done.'%(year, quarter))

def get_operation(year, quarter, q):
    """
        arturnover: 应收账款周转率
        arturndays: 应收账款周转天数
        inventory_turnover: 存货周转率
        inventory_days: 存活周转天数
        currentasset_turnover: 流动资产周转率
        currentasset_days: 流动资产周转天数
    """
    operation = ts.get_operation_data(year, quarter).drop_duplicates() \
                    .reindex(['code','arturnover','arturndays','inventory_turnover',
                              'inventory_days','currentasset_turnover','currentasset_days'], axis=1)
    res = operation.set_index(['code'], drop=True)
    q.put({'name': 'operation', 'data': res.to_dict('index')})
    logger.info('Fetch data \'operation\' for %s-%s ... Done.'%(year, quarter))

def get_growth(year, quarter, q):
    """
        mbrg: 主营业务增长率
        nprg: 净利润增长率
        nav: 净资产增长率
        targ: 总资产增长率
        epsg: 每股收益增长率
        seg: 股东权益增长率
    """
    growth = ts.get_growth_data(year, quarter).drop_duplicates().reindex(['code','mbrg','nprg','nav','targ','epsg','seg'], axis=1)
    res = growth.set_index(['code'], drop=True)
    q.put({'name': 'growth', 'data': res.to_dict('index')})
    logger.info('Fetch data \'growth\' for %s-%s ... Done.'%(year, quarter))

def get_debt(year, quarter, q):
    """
        currentratio: 流动比率
        quickratio: 速动比率
        cashratio: 现金比率
        icratio: 利息支付倍数
        sheqratio: 股东权益比率
        adratio: 股东权益增长率
    """
    debt = ts.get_debtpaying_data(year, quarter).drop_duplicates() \
             .reindex(['code','currentratio','quickratio','cashratio','icratio','sheqratio','adratio'], axis=1)
    if not debt.empty:
        debt.replace(to_replace='--', value=np.nan, inplace=True)
    res = debt.set_index(['code'], drop=True)
    q.put({'name': 'debt', 'data': res.to_dict('index')})
    logger.info('Fetch data \'debt\' for %s-%s ... Done.'%(year, quarter))

def get_cashflow(year, quarter, q):
    """
        cf_sales: 现金净流量/销售收入
        rateofreturn: 资产经营现金流量回报率
        cf_nm: 现金净流量/净利润
        cf_liabilities: 现金净流量/负债
        cashflowratio: 现金流量比率
    """
    cf = ts.get_cashflow_data(year, quarter).drop_duplicates() \
           .reindex(['code','cf_sales','rateofreturn','cf_nm','cf_liabilities','cashflowratio'], axis=1)
    if not cf.empty:
        cf.replace(to_replace='', value=np.nan)
    res = cf.set_index(['code'], drop=True)
    q.put({'name': 'cash_flow', 'data': res.to_dict('index')})
    logger.info('Fetch data \'cash_flow\' for %s-%s ... Done.'%(year, quarter))

# Main logic: collect last 10 years (about 40 quarters) data to database
# 2010.1 2010.2 2010.3 2010.4 ...
def init():
    hs = get_basics().reindex(['name', 'code'], axis=1)
    logger.info('Start init collect basic data.')
    _uuid = [ dict((('guid',uuid.uuid4()),)) for _ in range(len(hs))]
    _code = [ dict(((k,v),)) for k,v in zip(['code'] * len(hs), hs['code']) ]
    _name = [ dict(((k,v),)) for k,v in zip(['name'] * len(hs), hs['name']) ]
    holder = [ {**u, **x, **y, **z} for u,x,y,z in zip(_uuid, _code, _name, [copy.deepcopy({'profit':[], 'operation':[], 'growth':[], 'debt':[], 'cash_flow':[]}) for _ in range(len(hs))] ) ]
    # {'code': 'XXX', 'name': 'XXX', 'profit': [], 'operation': [], 'growth': [], 'debt': [], 'cash_flow': []}

    for _ye in range(2008, YEAR+1):
        for _qt in [1,2,3,4]:
            if _ye == YEAR and _qt == QUARTER:
                break
            _q = Queue()
            _q_ = Queue()
            d_set = dict()
            date = str(_ye) + '-' + str(_qt)
            logger.info('Start collecting date for: %s'%date)
            Thread(target=get_profit, args=(_ye, _qt, _q), name='Report:'+date).start()
            Thread(target=get_profit, args=(_ye, _qt, _q_), name='Report~:'+date).start()

            Thread(target=get_operation, args=(_ye, _qt, _q), name='Report:'+date).start()
            Thread(target=get_operation, args=(_ye, _qt, _q_), name='Report~:'+date).start()

            Thread(target=get_growth, args=(_ye, _qt, _q), name='Report:'+date).start()
            Thread(target=get_growth, args=(_ye, _qt, _q_), name='Report~:'+date).start()

            Thread(target=get_debt, args=(_ye, _qt, _q), name='Report:'+date).start()
            Thread(target=get_debt, args=(_ye, _qt, _q_), name='Report~:'+date).start()

            Thread(target=get_cashflow, args=(_ye, _qt, _q), name='Report:'+date).start()
            Thread(target=get_cashflow, args=(_ye, _qt, _q_), name='Report~:'+date).start()

            # Queue: { 'name': key, 'data': Dict }
            for i in range(5):
                _data = _q.get()
                _data_ = _q_.get()
                d_set[_data['name']] = {**d_set[_data['name']], **_data['data']}
                # d_set[_data['name']] = _data['data']


            for item in holder:
                _code = item['code']
                if d_set['profit'].get(_code) is not None:
                    item['profit'].append({date: d_set['profit'].get(_code)})
                else:
                    item['profit'].append({date: None})

                if d_set['operation'].get(_code) is not None:
                    item['operation'].append({date: d_set['operation'].get(_code)})
                else:
                    item['operation'].append({date: None})

                if d_set['growth'].get(_code) is not None:
                    item['growth'].append({date: d_set['growth'].get(_code)})
                else:
                    item['growth'].append({date: None})

                if d_set['debt'].get(_code) is not None:
                    item['debt'].append({date: d_set['debt'].get(_code)})
                else:
                    item['debt'].append({date: None})

                if d_set['cash_flow'].get(_code) is not None:
                    item['cash_flow'].append({date: d_set['cash_flow'].get(_code)})
                else:
                    item['cash_flow'].append({date: None})

    for item in holder:
        _profit = str(item['profit']).replace('\'', '"').replace('nan', 'null').replace('None', 'null')
        _operation = str(item['operation']).replace('\'', '"').replace('nan', 'null').replace('None', 'null')
        _growth = str(item['growth']).replace('\'', '"').replace('nan', 'null').replace('None', 'null')
        _debt = str(item['debt']).replace('\'', '"').replace('nan', 'null').replace('None', 'null')
        _cash_flow = str(item['cash_flow']).replace('\'', '"').replace('nan', 'null').replace('None', 'null')
        db_o.insert(guid=item['guid'], name=item['name'], code=item['code'],
                    profit=_profit, operation=_operation, growth=_growth, debt=_debt, cash_flow=_cash_flow).execute()

    db_o.commit()
    db_o.close()

def append(year, quarter):
    # TODO Append logic to get basic data for one spec quarter

