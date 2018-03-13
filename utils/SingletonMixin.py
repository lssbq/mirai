# -*- coding:utf-8 -*-
import threading


class SingletonMixin(object):
    """
    Thread safe singelton class
    Always use as Parent class
    """
    __singleton_lock = threading.Lock()
    __singleton_instance = None
    
    @classmethod
    def instance(cls):
        if not cls.__singleton_instance:
            with cls.__singleton_lock:
                if not cls.__singleton_instance:
                    cls.__singleton_instance = cls()
        return cls.__singleton_instance


if __name__ == '__main__':
    class A(SingletonMixin):
        pass

    class B(SingletonMixin):
        pass

    a1 = A.instance()
    a2 = A.instance()
    print(a1)
    print(a2)
    print(a1 is a2)