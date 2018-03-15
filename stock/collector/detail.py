# -*- coding:utf-8 -*-
"""
Collect details for single stock
"""

import numpy as np
import pandas as pd
import tushare as ts
import datetime as _dt
import re
from urllib.error import HTTPError
from Logger.Logger import Logger

log = Logger('lib')

def get_detail(code, date=None):
    """获取个股三年历史数据"""
    try:
        log.trace("Fetch history data for single stock: %s"%code)
        if date == None:
            _data = ts.get_hist_data(code, retry_count=6)
            if _data is not None:
                return _data.reset_index()
            else:
                return pd.DataFrame()
        else:
            _data = ts.get_hist_data(code=code, start=date, retry_count=6)
            if _data is not None:
                return _data.reset_index()
            else:
                return pd.DataFrame()
    except Exception as err:
        log.error("Could not fetch details for: %s"%code)
        log.error('Caused by: %s'%str(err))
        raise err

