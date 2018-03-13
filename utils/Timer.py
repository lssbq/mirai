# -*- coding:utf-8 -*-
import time
from Logger.Logger import Logger

# root Logger as default logger
# Create timer object as default timer

class Timer(object):
    def __init__(self, timer=time.perf_counter):
        self._func = timer
        self.elapse = 0.0
        self._start = None

    def start(self):
        if self._start is not None:
            raise RuntimeError('Timer already started!ß')
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
TIMER = Timer()
def LogTime(logger=LOGGER, timer=TIMER):
    """
    Usage : 
        @LogTime(logger, timer)
        def function()
    As decorator to log a function time with default timer 'perf_counter'
    You can pass your own timer and logger 
    """
    class _LogTime(object):
        def __init__(self, func):
            self._func = func
            self._logger = logger
            self._timer = TIMER

        def __call__(self, *args):
            with self._timer:
                self._func(*args)
            self._logger.info('[%s] Finished in %.3fms'%(self._func.__name__, self._timer.elapse*1000))
    return _LogTime


if __name__ == '__main__':
    @LogTime()
    async def test1():
        for i in range(100000000):
            i  = i ^ 2

    @LogTime()
    async def test2():
        for i in range(1000000):
            i  = i ^ 2

    test1()
    test2()