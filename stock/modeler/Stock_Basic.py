# -*- coding:utf-8 -*-
import math
import decimal

_PREC = 3
_ROUNDING = devimal.ROUND_HALF_EVEN
_CAPITALS = 0
_ROUNDING = decimal.ROUND_HALF_EVEN

# Set Decimal global context
def Context(*, prec):
    ctx = decimal.getcontext()
    ctx.capitals = _CAPITALS
    ctx.rounding = _ROUNDING
    if int(prec) is int:
        _PREC = prec
        ctx.prec = prec
    else:
        ctx.prec = _PREC



class Price():
    """ Stock price property:
            open, close , high, low
            price_change, p_change"""
    def __init__(self):

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
        return self._price_change

    @property
    def p_change(self):
        return self._p_change