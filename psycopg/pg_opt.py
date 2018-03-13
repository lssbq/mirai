# -*- coding:utf-8 -*-
# Need install psycop2 for Python 3
import psycopg2 as pg
from . import db_configuration as conf
from Logger.Logger import Logger
from utils.SingletonMixin import SingletonMixin

log = Logger('db')

class DB():
    class _db(SingletonMixin):
        t_status = {0:'TRANSACTION_STATUS_IDLE', 1:'TRANSACTION_STATUS_ACTIVE', 2:'TRANSACTION_STATUS_INTRANS', 3:'TRANSACTION_STATUS_INERROR', 4:'TRANSACTION_STATUS_UNKNOWN'}            
        c_status = {0:'STATUS_READY', 1:'STATUS_BEGIN', 2:'STATUS_IN_TRANSACTION', 3:'STATUS_PREPARED'}
        
        def __init__(self):
            log.trace('Starting connect to database.')
            self.con = self.con_init()
            self.cursor = self.con.cursor()

        # Create connection to db
        def con_init(self):
            log.info('Database connection init...')
            try:
                log.trace('Database type: %s'%conf.db_type)
                log.trace('Generated "dsn": "%s"'%conf.dsn)
                self.con = pg.connect(conf.dsn)
                self.con.autocommit = conf.autocommit
            except Exception as err:
                log.error('Failed to connect to database. %s'%err)

            log.trace('Successfully connected to database: \n\thost: %s:%s\n\tdb: %s\n\tuser: %s' %(conf.host, conf.port, conf.db_name, conf.user))
            log.trace('Server version: %s; encoding: %s'%\
                (self.con.get_parameter_status('server_version'), self.con.get_parameter_status('server_encoding')))
            log.info('Successfully connected to database')
            # Open a Cursor to operate database
            return self.con

        def execute(self, sql: str):
            if self.cursor and not self.cursor.closed:
                log.trace('Starting execute: "%s"'%sql)
                try:
                    self.cursor.execute(sql)
                except pg.InternalError as err:
                    log.error('Error occured when exectuing command: %s'%sql)
                    log.error(str(err))
                    self.rollback()
            else:
                log.error('Cursor is closed, can not execute sql exp.')
            log.trace('Finished to execute sql.')
            return self
        
        def fetch(self):
            return self.cursor.fetchall()

        def get_con(self):
            return self.con

        def commit(self):
            """
            Transaction status in Psycopg2:
            0 - TRANSACTION_STATUS_IDLE
                        No transaction and session idle
            1 - TRANSACTION_STATUS_ACTIVE
                        Command currently in progress
            2 - TRANSACTION_STATUS_INTRANS
                        Session idle in a valid transaction
            3 - TRANSACTION_STATUS_INERROR
                        Session idle in a faild transaction
            4 - TRANSACTION_STATUS_UNKNOWN
                        Connection is bad
            """
            _status = self.con.get_transaction_status()
            log.trace('Current transaction status: ' + self.t_status[_status])
            if self.con.closed == 0:
                if _status in [1,2]:
                    self.con.commit()
                    log.info('Session successfully commited!')
                else:
                    log.info('Session no need to commit...')
            else:
                log.error('Connection already closed, nothing to commit!')

        def rollback(self):
            """Rollback current transaction if any Exception occurs"""
            _status = self.con.get_transaction_status()            
            log.error('Transaction will rollback because some unexpected status')
            log.trace('Current connection status:' + self.c_status[self.con.status])
            log.trace('Current transaction status: ' + self.t_status[_status])
            if self.con.closed == 0:
                self.con.rollback()
                log.error('Session has been rollback!')

    # Singleton the DB connection
    # instance = None
    def __init__(self):
        pass
        # if DB.instance is None:
        #     DB.instance = DB._db()

    def get_exec(self):
        # return DB.instance
        return DB._db.instance()



if __name__ == '__main__':
    sql = """CREATE TABLE IF NOT EXISTS basic.stock_basic (
                guid    uuid NOT NULL UNIQUE,
                code    varchar(20) NOT NULL ,
                name    varchar(20) NOT NULL,
                industry    varchar(50),
                concept varchar(200),
                sme     boolean DEFAULT false,
                gem     boolean DEFAULT false,
                st      boolean DEFAULT false,
                hs300   double precision,
                sh50    boolean DEFAULT false,
                zz500   boolean DEFAULT false,
                pe      double precision,
                outstanding     double precision,
                totals          double precision,
                total_assets    double precision,
                liquid_assets   double precision,
                time_to_market  date,
                pb              double precision,
                rev             double precision,
                profit          double precision,
                holders         integer,
                
                PRIMARY KEY(code),
                CHECK(holders >= 0::integer )
            );
        """

    db1 = DB().get_exec()
    db2 = DB().get_exec()
    print(db1)
    print(db2)
    # Build an exception
    # db.execute('SELECT * FROM detail.ii;')
    # db.execute('SELECT * FROM meta.index;')
    # db.rollback()