"""
NoData Exception:
    Wrapper of tushare lib
    表示希望获取的数据不存在(tushare return HTTPError 404)
    通常由于时间异常或者当天非交易日
"""
import datetime as _dt

NOTFOUND_CODE = 404

class NoData(Exception):
    def __init__(self, date: str, message: str):
        super(Exception, self).__init__(message)
        self._code = NOTFOUND_CODE
        self.date = date
    
    def __str__(self):
        return '(' + str(NOTFOUND_CODE) + ') ' + self.date + r' date does not exists.'

    def getDatetime(self):
        return _dt.datetime.strptime(self.date, r'%Y-%m-%d').date()
