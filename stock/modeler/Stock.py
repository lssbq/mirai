# -*- coding:utf-8 -*-
import math
from .Candle import _Candle
from decimal import Decimal as _Decimal


VALUE_PREC = '.001'
RATIO_PREC = '.00001'


def Decimal(value: str, prec = VALUE_PREC):
    vlaue = str(value)
    return _Decimal(value).quantize(_Decimal(prec))


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
        days = int(days)
        attr = 'ma%s'%days
        res = getattr(self.candle[0].ma, attr) # Could not use '[attr]' to get attr with __getattr__
        if res is None:
            for i in range(days-1):
                setattr(self.candle[i].ma, attr, 0)
            
            total = len(self.candle)
            for j in range((days-1), total, 1):
                summ = Decimal(0)
                for step in range(days):
                    summ += self.candle[j-step].close
                setattr(self.candle[j].ma, attr, Decimal(summ/days))

    def daily_return(self, days=1):
        """Calculare Return for specific period, default is daily."""
        assert int(days)>0, 'Return period must be positive number.'
        days = int(days)
        # {date:'2000-01-01' , return: ln(S/S')}
        res = list()
        for i in range(days, len(self.candle), days):
            date = str(self.candle[i].date)
            cal = math.log((self.candle[i].close / self.candle[i-days].close))
            ret = Decimal(cal, RATIO_PREC)
            res.append({'date': date, 'return': ret})
        return res