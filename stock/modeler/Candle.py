# -*- coding:utf-8 -*-
"""
Represent stock candlestick for each day:
open, close , high, low
price_change, p_change
"""
import math
from .Ma import _MA
from decimal import Decimal as _Decimal


CANDLE = ['open', 'close', 'high', 'low', 'price_change', 'p_change', 'volume', 'turnover']
FIELDS = ['date', 'open', 'high', 'close', 'low', 'volume', 'price_change', 'p_change', 'turnover']

VALUE_PREC = '.001'
RATIO_PREC = '.00001'


def Decimal(value: str, prec = VALUE_PREC):
    vlaue = str(value)
    return _Decimal(value).quantize(_Decimal(prec))


# Candel class: daily basic stock informations
#   Including basic status
#   Some calculated informations:
#       rose:           (B)price grows up or fall down (1, 0, -1)
#       raise_limit:    (B)price raise to the limit +10%
#       limit_down:     (B)price fall down to the limit -10%
#       open_high:      (R)opening price high than close price yesterday
#       amplitude:      (R)[high - low]/[pre_close]
#       u_ratio:        (R)upper shadow ratio in a day amplitude
#       l_ratio:        (R)lower shadow ratio in a day amplitude
class _Candle():
    def __init__(self, **kwargs):
        if len(kwargs) == len(FIELDS):
            for item in FIELDS:
                assert item in kwargs, 'Missing key args: \'%s\''%item
                if item in CANDLE:
                    val = Decimal(kwargs[item], VALUE_PREC)
                    setattr(self, '_'+item, val)

        self.date = kwargs['date']
        self.ma = _MA(**kwargs)

    @property
    def open(self):
        return self._open

    @property
    def close(self):
        return self._close
    
    @property
    def high(self):
        return self._high
    
    @property
    def low(self):
        return self._low
    
    @property
    def price_change(self):
        """
        The price: (today's close - yestday's close)
        """
        return self._price_change

    @property
    def pre_close(self):
        """Calculated be today: (close - price_change)"""
        return self._close - self._price_change

    @property
    def p_change(self):
        return self._p_change

    @property
    def volume(self):
        return self._volume

    @property
    def turnover(self):
        return self._turnover

    # Calculated attribute
    @property
    def rose(self):
        if self.close > self.pre_close:
            return 1
        elif self.close == self.pre_close:
            return 0
        else:
            return -1

    @property
    def raise_limit(self):
        pass

    @property
    def limit_down(self):
        pass

    @property
    def open_high(self):
        return Decimal((self.open - self.pre_close)/self.pre_close, RATIO_PREC)

    @property
    def open_low(self):
        return bool(self.open < self.pre_close)

    @property
    def amplitude(self):
        return Decimal((self.high - self.low)/self.pre_close, RATIO_PREC)

    @property
    def u_ratio(self):
        h_body = self.close if self.close > self.open else self.open
        total = self.high - self.low
        return Decimal((self.high - h_body)/total, RATIO_PREC)
    
    @property
    def l_ratio(self):
        l_body = self.close if self.close < self.open else self.open
        total = self.high - self.low
        return Decimal((l_body - self.low)/total, RATIO_PREC)

    def __str__(self):
        string = 'On %s, open: %s, high: %s, close: %s, low: %s, volume: %s'%(self.date,self.open,self.high,self.close,self.low,self.volume)
        return string
