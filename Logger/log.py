# -*- coding:utf-8 -*-

def log(*, dbname, tablename):
    class logger:
        '''Decorator of Psycopg2 library'''
        def __init__(self, fun):
            self.__fun__ = fun

        def __call__(self, *args):
            print('[SQL: %s in %s] %s execute...' % (tablename, dbname, self.__fun__.__name__.capitalize()))
            return self.__fun__(*args)

    return logger