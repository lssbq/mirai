# -*- coding:utf-8 -*-
"""
Move Average of a stock one day.
Nested in Candle object.
"""
from decimal import Decimal as _Decimal

MA = ['ma5', 'ma10', 'ma20', 'v_ma5', 'v_ma10', 'v_ma20']

def Decimal(value: str):
    vlaue = str(value)
    return _Decimal(value).quantize(_Decimal('.001'))


class _MA:
    def __init__(self, **kwargs):
        object.__setattr__(self, 'fields', dict())
        for item in MA:
            assert item in kwargs, 'Missing key arg: \'%s\''%item
            val = Decimal(kwargs[item])
            setattr(self, item, val)
    
    def __setattr__(self, name, value):
        f = object.__getattribute__(self, 'fields')
        if f.get(name, None) is None:
            f[name] = value
        else:
            raise ValueError('MA \'%s\' already exists!'%name)
    
    def __getattribute__(self, name):
        return object.__getattribute__(self, 'fields').get(name, None)