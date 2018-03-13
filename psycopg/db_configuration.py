import json
import re
from Error.ConfigError import ConfigError
from Logger.Logger import Logger


log = Logger('db')

db_type = None
host = None
port = None
user = None
password = None
db_name = None
autocommit = False
dsn=''

log.trace('Read db configuration file: \'connection.json\'')
with open('./conf/connection.json', mode='r', encoding='utf-8') as fp:
    conf = json.load(fp)
    log.trace('JSON loaded: %s'%conf)
    try:
        db_type = conf['type']
        host = conf['host']
        _port = str(conf['port'])
        if not re.match(r'\d{2,5}', _port):
            raise ConfigError('port', 'Format not allowed, unexpected number!')
        port = _port
        user = conf['user']
        password = conf['passwd']
        db_name = conf['db']
        autocommit = conf['autocommit']

    except KeyError as err:
        raise ConfigError(err, 'Undefined key or unrecognized key name!')

    dsn = "host='%s' port='%s' dbname='%s' user='%s' password='%s'"%(host, port, db_name, user, password)


if __name__ == '__main__':
    print('db_type: %s' % db_type)
    print('host: %s' % host)
    print('port: %s' % port)
    print('user: %s' % user)
    print('password: %s' % password)
    print('db_name: %s' % db_name)
    