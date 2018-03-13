# -*- coding:utf-8 -*-
"""
A non-thread-safe decerator to singleton a class.
Should be used as a decorator:
    @Singleton
"""
class Singleton():
    
    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)