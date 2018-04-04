# -*- coding:utf-8 -*-
from .Candle import _Candle
from psycopg import schema
from decimal import Decimal as _Decimal


def Decimal(value: str):
    vlaue = str(value)
    return _Decimal(value).quantize(_Decimal('.00001'))


# Attributes:
#  guid, code, name, industry
#  sme, gem, st
#  hs300, sh50, zz500
#  pe, pb, total_assets
#  holders
class Stock():
    def __init__(self, basic, daily):
        # self.date = kwargs['date']
        # Set basic attributes for self
        for k,v in basic.items():
            setattr(self, k, v)
        # Build Candel list
        self.candle = [_Candle(**item) for item in daily]


def ma_cal(self, days):
    """Calculate specific days move average.
          N days close price/N"""
    assert int(days) > 0, 'Move Average time interval must be positive number.'
    for i in range(days):
        # TODO Calculate the first days MA
    
    for j in range(len(self.candle) - days):
        # TODO Calculate MA


if __name__ == '__main__':
    # res = schema.get_schema('DETAIL_MODEL').set_table('s_000001').select().execute().fetch()
    con = schema.get_con()

    res = schema.get_schema('DETAIL_MODEL').set_table('s_000001').select().execute().fetch()
    s = Stock({'pe':12, 'holders':130000}, res)