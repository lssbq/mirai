"""
Read the logger configure file to initial Logger instance
"""
import json
from Error.ConfigError import ConfigError

LOG_LEVEL = {'trace': 'TRACE', 'info': 'INFO ','debug':'DEBUG', 'error':'ERROR'}
LEVEL_CMP = {'trace': 1, 'info': 2,'debug': 3, 'error':4}
ROOT_LEVEL = None
PATH = None
DB_LEVEL = None
Lib_LEVEL = None
SPLIT = False
CONSOLE = False

with open('./conf/logger.json', mode='r', encoding='utf-8') as fp:
    conf = json.load(fp)

    try:
        ROOT_LEVEL = conf['rootLevel']
        DB_LEVEL = conf['db']['level']
        Lib_LEVEL = conf['lib']['level']

        PATH = conf['path']
        SPLIT = conf['split']
        CONSOLE = conf['consoleEnable']

    except KeyError as err:
        raise ConfigError(err, 'Undefined key or unrecognized key name!')


if __name__ == '__main__':
    print(ROOT_LEVEL)
    print(DB_LEVEL)
    print(Lib_LEVEL)
    print(PATH)
    print(SPLIT)
