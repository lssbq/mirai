"""
Logger package for db, lib 
"""
from . import LoggerConf
import datetime as dt
import os


LOG_FILE_NAME = 'output.log'

class Logger():
    class _ANSI_COLOR:
        """
        ANSI escape sequences to print color in console output.
        Works on 8 colors terminal:
            Linux
            Unix(OS X)
            Windows 10 ???
        """
        TIME = '\u001b[36m'
        TRACE = '\u001b[34m'
        INFO = '\u001b[32m'
        DEBUG = '\u001b[33m'
        ERROR = '\u001b[31m'
        ENDC = '\u001b[0m'

        def disable(self):
            self.TIME = ''
            self.TRACE = ''
            self.INFO = ''
            self.DEBUG = ''
            self.ERROR = ''
            self.ENDC = ''

    class _Logger():
        def __init__(self, conf):
            self.now = dt.date.today()
            self.conf = conf
            self.fp = self._file()
            
        def _file(self):
            """
            File split and open log file
            """
            _path = (self.conf.PATH+LOG_FILE_NAME if (self.conf.PATH[-1:-2] == '/') else self.conf.PATH+'/'+LOG_FILE_NAME)
            if os.path.isfile(_path) and self.conf.SPLIT:
                if hasattr(self, 'fp') and not self.fp.closed:
                    self.fp.close()
                timestamp = dt.datetime.now().strftime(r'-%Y-%m-%d-%H:%M')
                os.rename(_path, _path + timestamp)

            try:
                return open(_path, 'a+')
            except FileNotFoundError:
                return open(_path, 'w')


        def _write_f(self, lv:str, msg: str):
            """
            Write to log file:  First check if SPLIT or not and now date
            """
            if self.conf.SPLIT and dt.date.today() > self.now:
                self._file(self)
                self.now = dt.date.today()

            time_pf = dt.datetime.now().__str__()
            self.fp.write(time_pf + ' ' + lv + msg + '\n')
            # Console enable
            if self.conf.CONSOLE:
                # Colorful ~~~~
                _lv = lv[1:-1]
                _lv_col = getattr(Logger._ANSI_COLOR, _lv) + lv + Logger._ANSI_COLOR.ENDC
                print(Logger._ANSI_COLOR.TIME + time_pf + Logger._ANSI_COLOR.ENDC + ' ' + _lv_col + msg)

        def trace(self, msg: str):
            _lv = '[' + LoggerConf.LOG_LEVEL['trace'] + ']'
            self._write_f(_lv, msg)

        def info(self, msg: str):
            _lv = '[' + LoggerConf.LOG_LEVEL['info'] + ']'
            self._write_f(_lv, msg)
        
        def debug(self, msg: str):
            _lv = '[' + LoggerConf.LOG_LEVEL['debug'] + ']'
            self._write_f(_lv, msg)

        def error(self, msg: str):
            _lv = '[' + LoggerConf.LOG_LEVEL['error'] + ']'
            self._write_f(_lv, msg)
    
    logger = None

    def __init__(self, server: str='rt'):
        if not Logger.logger:
            Logger.logger = Logger._Logger(LoggerConf)
        self.server = server
        self.srv_pf = ' [' + server.upper() + '] '

    def _enable(self, level):
        if self.server == 'db' and (LoggerConf.LEVEL_CMP[LoggerConf.DB_LEVEL] <= LoggerConf.LEVEL_CMP[level]):
            return True
        elif self.server == 'lib' and (LoggerConf.LEVEL_CMP[LoggerConf.Lib_LEVEL] <= LoggerConf.LEVEL_CMP[level]):
            return True
        elif (LoggerConf.LEVEL_CMP[LoggerConf.ROOT_LEVEL] <= LoggerConf.LEVEL_CMP[level]):
            return True
        
        return False

    def trace(self, msg):
        if self._enable(level='trace'):
            Logger.logger.trace(self.srv_pf + msg)
    
    def info(self, msg):
        if self._enable(level='info'):
            Logger.logger.info(self.srv_pf + msg)

    def debug(self, msg):
        if self._enable(level='debug'):
            Logger.logger.debug(self.srv_pf + msg)

    def error(self, msg):
        if self._enable(level='error'):
            Logger.logger.error(self.srv_pf + msg)




if __name__ == '__main__':
    log = Logger('db')
    log.trace('test db error log')
    print(Logger.logger)
    lg = Logger('lib')
    lg.debug('test lib error log')
    lg.error('test lib error log')
    lg.trace('test lib error log')
    print(Logger.logger)
    Logger().info('test lib error log')