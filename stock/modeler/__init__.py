# -*- coding:utf-8 -*-
from decimal import getcontext, Decimal, ROUND_HALF_UP

_PREC = 30
_ROUNDING = ROUND_HALF_UP
_CAPITALS = 0

# Set Decimal global context for modeler package
ctx = getcontext()
ctx.capitals = _CAPITALS
ctx.rounding = _ROUNDING
ctx.prec = _PREC
