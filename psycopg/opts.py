# For test INSERT command in PostgreSQL
from log import log

@log(dbname='stock', tablename='test')
def execute(cur):
    if (cur is not None) and cur.closed:
        print('Cursor is closed...')
        return

    print('Cursor opened: %s' % (not cur.closed))
    # connection object get from cursor
    print('Connection: %s' % cur.connection.dsn)
    # Server side cursor / client side cursor
    print('Cursor name: %s' % cur.name)
    # RW: If a named cursor lifetime should extend outside of the current transaction
    print('Cursor lifetime is hold: %s' % cur.withhold)
    # RW: Can scroll backwards
    print('Cursor can backwards: %s' %cur.scrollable)


@log(dbname='stock', tablename='test')
def finish(cur):
    # print('[Finish] Closing ... ')
    con = cur.connection
    # Close the connection
    cur.close()
    con.close()

@log(dbname='stock', tablename='test')
def select(cur):
    # Execute a sql command
    # No returned result
    cur.execute("SELECT pg_database_size(current_database());")
    cur.execute("SELECT * FROM test;")

@log(dbname='stock', tablename='test')
def getResult(cur):
    print('[Fetch] Fetching ... ')
    # Fetch sql result from Cursor
    rows = cur.fetchall()
    print(rows)
