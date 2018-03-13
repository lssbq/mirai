# -*- coding:utf-8 -*-
"""
Get schema from con file schema.json
This schema returns a SQL command, not the same meaning of Postgre schema.
"""
import json
from Error.TypeError import TypeError,SchemaNotFound
from utils.Singleton import Singleton
from Logger.Logger import Logger
from .value import value as sql_value
from .pg_opt import DB

log = Logger('db')
__SCHEMA = dict()


class Schema(object):
    class __Field(object):
        def __init__(self, _key, _type, _name):
            self.key = _key
            self.type = _type
            self.name = _name
        @property
        def value(self):
            return self.__value
        @value.setter
        def value(self, _v):
            self.__value = sql_value(_v, self.type)

    @Singleton
    class __exec():
        def __init__(self):
            self.db = DB().get_exec()
    
        def __getattr__(self, attr):
            return getattr(self.db, attr)

    def __init__(self, schema):
        self.name = list(schema.keys())[0] # Should always contents one object
        _conf = schema[self.name]
        self.abstract = _conf.get('abstract', False)
        self.ready = not self.abstract
        self.schema = _conf['schema']
        self.table = _conf['table']
        self.fields = dict()
        self._exec = self.__exec.Instance()
        self.__sql = ''
        for key in _conf['fields']:
            self.fields[key] = self.__Field(key, _conf['fields'][key], self.name)

    def __key_value(self, **args):
        _sql_key = ''
        _sql_val = ''
        for key in args:
            self.fields[key].value = args[key]
            if not _sql_key:
                _sql_key += '(' + key
                _sql_val += '(' + self.fields[key].value
            else:
                _sql_key += ',' + key
                _sql_val += ',' + self.fields[key].value
        return _sql_key+') VALUES'+_sql_val+')'

    def __check_ready(self):
        # Raise error if not set table name for abstract schema
        if self.abstract and not self.ready:
            log.error('Abstract SCHEMA must be set table name before execute.')
            raise Exception('Abstract SCHEMA must be set table name before execute.')
        # Once passed check ready, set the ready to False: Need to set table name before each operation
        if self.abstract:
            self.ready = False

    def execute(self):
        if self.__sql:
            log.trace('Execute SQL: [[%s]]'%self.__sql)
            _self = self._exec.execute(self.__sql)
            self.__sql = ''
        else:
            raise Exception('No operation will be executed, please select an SQL operation before.')
        return _self

    def select(self, fields: list, exp: str = None, **args):
        self.__check_ready()
        log.info('SQL SELECT will be excuted to fetch %s with : %s, %s'%(fields, exp, args))
        log.trace('Build SELECT expression in %s.%s' %(self.schema, self.table))
        if exp:
            self.__sql = 'SELECT '+ ','.join(fields) +' FROM ' + self.schema + '.' +self.table + ' WHERE ' + exp + ';'
        elif not args:
            self.__sql = 'SELECT '+ ','.join(fields) +' FROM ' + self.schema + '.' +self.table + ';'
        else:
            _expr = ''
            for k,v in args.items():
                _expr += k+'='+v+' AND '
            self.__sql = 'SELECT '+ ','.join(fields) +' FROM ' + self.schema + '.' +self.table + ' WHERE ' + _expr[:-5] + ';'
        return self

    def insert(self, **args):
        self.__check_ready()
        log.trace('Build INSERT expression in %s.%s' %(self.schema, self.table))        
        self.__sql = 'INSERT INTO ' + self.schema + '.' +self.table + self.__key_value(**args) + ';'
        return self

    def update(self, uuid, **args):
        self.__check_ready()
        log.info('SQL UPDATE will be excuted to update "%s.%s" with : %s'%(self.schema, self.table, args))
        log.trace('Build UPDATE expression in %s.%s' %(self.schema, self.table))
        _sql = ''
        for key in args:
            self.fields[key].value = args[key]
            _sql += key + '=' + self.fields[key].value + ','

        self.__sql = 'UPDATE ' + self.schema + '.' +self.table + ' SET ' + _sql[:-1] + " WHERE guid='" + uuid + "';"
        return self

    def delete(self, exp: str):
        self.__check_ready()
        log.info('SQL DELETE will be excuted to drop "%s.%s" with : %s'%(self.schema, self.table, exp))
        log.trace('Build DELETE expression in %s.%s' %(self.schema, self.table))        
        self.__sql = 'DELETE FROM ' + self.schema + '.' +self.table + ' WHERE ' + exp + ';'
        return self

    def get_table(self):
        self.__check_ready()
        return self.schema + '.' + self.table

    def set_table(self, name):
        if self.abstract:
            if self.table.find(r'%s') >= 0:
                self.table = self.table%(name)
            else:
                self.table = name
            self.ready = True
        return self

    def create_table(self):
        self.__check_ready()
        log.info('Prepare to create table in SCHEMA: %s.s_%s' %(self.name, self.name))
        _sql = "CREATE TABLE IF NOT EXISTS %s (%s);"%(self.schema + '.' + self.table, self.__field_create()[:-1])
        self.__sql = _sql
        return self

    def __field_create(self):
        _sql = ''
        for item in self.fields.values():
            _sql += item.key + ' ' + item.type + ','
        return _sql

    def exist(self, t_name):
        _sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables\
                 WHERE table_schema='%s' AND table_name='%s');"%(self.schema, t_name)
        self.__sql = _sql
        return self

    def get_sql(self):
        """ Return the sql command as string, 
            normally used to debug or run the command manually """
        return self.__sql

    def get_con(self):
        # Always return psycopg connection object, no dependence of Schema instance
        return self._exec.get_con()



# Read schema configuration form json file
log.info('Starting load SCHEMA from \'schema.json\'')
with open('./conf/schema.json', mode='r', encoding='utf-8') as fp:
    __schemas = json.load(fp)
    log.info(' %s SCHEMAS in configuration file.'%len(__schemas))
    # Build from json configurations in a single loop
    for s in __schemas:
        log.info('SCHEMA:: %s'%s)
        __SCHEMA[list(s.keys())[0]] = Schema(s)

def get_schema(name: str):
    """
    Return the Schema to build SQL command.
    """
    if name in __SCHEMA:
        return __SCHEMA[name]
    else:
        log.error('Get schema: \'%s\' not found.' % (name))
        raise SchemaNotFound(name)


__all__ = ['get_schema']


#=====================================
if __name__ == '__main__':
    # schema = get_schema('BASIC_STOCKS').update('cf52cbc2-126a-4fdc-ab15-dc52f3d266d9',**{'totals': '123', 'pe': '234', 'holders':'123.345', 'time_to_market': None})
    # select = get_schema('BASIC_STOCKS').select(['guid', 'code', 'name'],**{'totals': '123', 'pe': '234', 'holders':'123.345'})
    # select = get_schema('BASIC_STOCKS').select(['guid', 'code', 'name'])
    # create = get_schema('DETAIL_MODEL').set_table('600001').create_table()
    # print(schema)
    # print(select)
    # print(create)
    db = DB().get_exec()
    print(db)
    db.execute('SELECT * FROM meta.index;')
    # res = get_schema('META_INDEX').select(['guid', 'code']).execute()
    res = get_schema('META_INDEX').get_con()
    print(res)