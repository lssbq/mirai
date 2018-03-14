# -*- coding:utf-8 -*-
import datetime as dt
import sys, time
import schedule
from .Daemon import Daemon


class Task(Daemon):
    def work(self):
        print('Daemon running, now is %s'%dt.datetime.now().isoformat())

    def run(self):
        schedule.every(10).seconds.do(self.work).tag('Demo-task', 'test1')
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    daemon = Task('/var/run/mirai.pid', stdout='/var/log/mirai/stdout', stderr='/var/log/mirai/stderr')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)
