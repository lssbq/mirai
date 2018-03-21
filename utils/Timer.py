# -*- coding:utf-8 -*-
import time
from Logger.Logger import Logger
from copy import deepcopy

# root Logger as default logger
# Create timer object as default timer

class Timer(object):
    def __init__(self, timer=time.perf_counter):
        self._func = timer
        self.elapse = 0.0
        self._start = None

    def start(self):
        if self._start is not None:
            raise RuntimeError('Timer already started!')
        self._start = self._func()

    def stop(self):
        if self._start is None:
            raise RuntimeError('Timer not started!')
        end = self._func()
        self.elapse += end - self._start
        self._start = None
    
    def __enter__(self):
        self.elapse = 0.0
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()



LOGGER = Logger('root')
def LogTime(logger=LOGGER, timer=None):
    """
    Usage : 
        @LogTime(logger, timer)
        def function()
    As decorator to log a function time with default timer 'perf_counter'
    You can pass your own timer and logger 
    """
    if timer is None:
        timer = Timer()

    # class _LogTime(object):
    #     def __init__(self, func):
    #         self._func = func
    #         self._logger = logger
    #         self._timer = timer

    #     def __call__(self, *args, **kwargs):
    #         with self._timer:
    #             _res = self._func(*args, **kwargs)
    #         self._logger.info('[%s] Finished in %.3fms'%(self._func.__name__, self._timer.elapse*1000))
    #         return _res

    def Wrapper(func):
        def _wrap_method(*args, **kwargs):
            _timer = deepcopy(timer)
            with _timer:
                _res = func(*args, **kwargs)
            logger.info('[%s] Finished in %.3fms'%(func.__name__, _timer.elapse*1000))
            return _res
        return _wrap_method
    return Wrapper

