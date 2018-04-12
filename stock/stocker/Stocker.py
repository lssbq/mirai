# -*- coding:utf-8 -*-
"""
This is the stock model builder to fetch stock details and return Sotck object.
    You can build by sotck code or guid.
    Also can build the details in a specific time period.
"""
import numpy as np
import pandas as pd
import datetime as dt
from psycopg import schema
from stock.modeler.Stock import Stock


BASIC = ['guid', 'code', 'name', 'industry', 'sme', 'gem', 'st', 
         'hs300', 'sh50', 'zz500', 'pe', 'pb', 'total_assets', 'holders']

class Stocker():
    def __init__(self, code):
        assert code, 'Must provied stock code to fetch data!'
        self.stock = code
        self.basic = schema.get_schema('BASIC_STOCKS')
        self.detail = schema.get_schema('DETAIL_MODEL')
        self.init()

    def __from_db(self):
        # stock basic or not exists
        res = self.basic.select(code=self.stock).execute().fetch()
        assert len(res), '\'%s\' not exists.'%self.stock
        basic = {k: v for k, v in res[0].items() if k in BASIC}
        # stock daily detail
        t_name = 's_' + self.stock
        assert self.detail.set_table(t_name).exist(), '\'%s\' daily detail not exists.'%self.stock
        details = self.detail.set_table(t_name).select().execute().fetch()
        details_s = sorted(details, key=lambda obj: obj['date'], reverse=False)
        return {'basic': basic, 'details': details_s}

    def init(self):
        self.instance = None
        basic = self.__from_db()
        self.instance = Stock(basic['basic'], basic['details'])
        for i in (5, 10, 20):
            self.instance.ma_cal(i)

    def get(self) -> Stock:
        assert self.instance, 'Could not get stock \'%s\' details.'%self.stock
        return self.instance


if __name__ == '__main__':
    s = Stocker('000001')
    s.get()