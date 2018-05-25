# -*- coding: utf-8 -*-
"""
functools.lru_cache(maxsize=128, typed=False)

Cache function result in memory when called with same parameters
"""
def memcache(func):
    cache = dict()

    def wrapper(arg):
        try:
            return cache[arg]
        except KeyError as e:
            res = func(arg)
            cache[arg] = res
            return res

    return wrapper


if __name__ == '__main__':
    @memcache
    def test(a):
        print('Cal with params: %s'%a)
        return a**2

    print(test(2))
    print(test(3))
    print(test(2))