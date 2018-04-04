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
from stock.modeler import stock


class Stocker():
    def __init__(self, code):
        pass
