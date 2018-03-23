# -*- coding:utf-8 -*-
"""
Entry point of basic stock data cokllector
    Update stock and index data in database
    Collect data summary:
        new stocks
        stop/ST stocks
        updated stocks and index
"""
import time
import tushare as ts
import datetime as dt
import psycopg.pg_opt as pg
from threading import Thread
import psycopg.schema as schema
from Logger.Logger import Logger
from stock.collector import fill
from stock.collector import index
from urllib.error import HTTPError

log = Logger('lib')

today = dt.date.today().isoformat()
daily = schema.get_schema('DAILY')
RETRY_COUNT = 3


def _check_update():
    res = daily.select(["*"], date='\'%s\''%today).execute().fetch()
    if len(res):
        return True
    # Already done today
    return False


def _update_meta(**kargs):
    """
    {"status":1, "stocks":3507, "new_stocks":0, "index":26}
    """
    json_str = ''
    for k,v in kargs.items():
        json_str += '"%s"'%k + ':' + str(v) + ','
    json_str = '{' + json_str[:-1] + '}'
    daily.insert(**{'date': today, 'data': json_str}).execute()
    daily.commit()

def _job():
    f = Thread(target=fill.do, name='Init-fill')
    i = Thread(target=index.do, name='Init-index')
    f.start()
    i.start()


def _start():
    # is_holiday only accept ISO format: %Y-%m-%d
    if not ts.is_holiday(today) and _check_update():
        flag = False
        for i in range(RETRY_COUNT):
            try:
                ts.get_stock_basics(today)
                flag = True
                break
            except HTTPError as err:
                if err.getcode() == 404:
                    time.sleep(1800)
                    continue
        if flag:
            log.info('===============================================')
            log.info('Collector daily task \'%s\' start.'%today)
            _job()
        else:
            log.error('===============================================')
            log.error('Nothing to collect for today: %s'%today)
            _update_meta(status=0)
    else:
        log.info('===============================================')
        log.info('\'%s\' is holiday, nothing to update in database.')



def run():
    pass



if __name__ == '__main__':
    print(today)
    run()
