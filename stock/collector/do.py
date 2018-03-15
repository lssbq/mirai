# -*- coding:utf-8 -*-
"""
Entry point of basic stock data cokllector
    Update stock and index data in database
    Collect data summary:
        new stocks
        stop/ST stocks
        updated stocks and index
"""
import datetime as dt
import tushare as ts
import psycopg.pg_opt as pg
import psycopg.schema as schema
from stock.collector import fill
from stock.collector import index
from Logger.Logger import Logger

log = Logger('lib')

today = dt.date.today().isoformat()


def run():
    # is_holiday only accept ISO format: %Y-%m-%d
    if not ts.is_holiday(today):
        log.info('Collector daily task \'%s\' start.'%today)
        fill.do()
        index.do()
    else:
        log.info('\'%s\' is holiday, nothing to update in database.')



if __name__ == '__main__':
    print(today)
else:
    run()
