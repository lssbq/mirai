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
from queue import Queue
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
THREAD_NUM = 10
q = Queue()


def _check_update():
    res = daily.select(["*"], date='\'%s\''%today).execute().fetch()
    if len(res):
        return False
    # Not done today
    return True


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
    daily.close()

def _job():
    f = Thread(target=fill.do, args=(today, q, THREAD_NUM), name='Init-fill')
    i = Thread(target=index.do, args=(today, q), name='Init-index')
    f.start()
    i.start()

    _stocks = 0
    _new_stocks = 0
    _index = 0
    _status = 0
    for i in range(THREAD_NUM + 1):
        _data = q.get()
        _stocks += _data.get('stocks', 0)
        _new_stocks += _data.get('new', 0)
        _index += _data.get('index', 0)
    _status = 1
    _update_meta(status=_status, stocks=_stocks, new_stocks=_new_stocks, index=_index)


def _start():
    # is_holiday only accept ISO format: %Y-%m-%d
    if not ts.is_holiday(today):
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
        if flag and _check_update():
            log.info('===============================================')
            log.info('Collector daily task \'%s\' start.'%today)
            _job()
        else:
            log.error('===============================================')
            log.error('Nothing to collect for today: %s'%today)
            _update_meta(status=0)
    else:
        log.info('===============================================')
        log.info('\'%s\' is holiday, nothing to update in database.'%today)



def run():
    _start()



if __name__ == '__main__':
    print(today)
    run()
