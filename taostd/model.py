import logging
import threading
from queue import Empty


class Engine(object):

    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()


class TDCtx(threading.local):
    """
    Thread local object that holds connection info.
    """

    def __init__(self, pool):
        self.pool = pool
        self.connection = None

    def is_init(self):
        return self.connection is not None

    def init(self):
        self.connection = self.pool.get()
        logging.debug('use connection <%s>...' % hex(id(self.connection)))

    def cleanup(self):
        self.pool.put_nowait(self.connection)
        self.connection = None
        logging.debug('release connection <%s>...' % hex(id(self.connection)))

    def cursor(self):
        """
        Return cursor
        """
        return self.connection.cursor()

    def statement(self, sql):
        """
        Return statement
        """
        return self.connection.statement(sql)

    def __del__(self):
        try:
            while True:
                conn = self.pool.get_nowait()
                if conn:
                    conn.close()
                    logging.debug('close connection <%s>...' % hex(id(conn)))
        except Empty:
            pass


class ConnectionCtx(object):
    """
    ConnectionCtx object that can open and close connection context. _ConnectionCtx object can be nested and only the most
    outer connection has effect.
    with connection():
        pass
        with connection():
            pass
    """

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    def __enter__(self):
        self.should_cleanup = False
        if not self.db_ctx.is_init():
            self.db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        if self.should_cleanup:
            self.db_ctx.cleanup()


class TDError(Exception):
    pass


class MultiColumnsError(TDError):
    pass


class Dict(dict):
    """
    Simple dict but support access as x.y style.
    >>> d1 = Dict()
    >>> d1['x'] = 100
    >>> d1.x
    100
    >>> d1.y = 200
    >>> d1['y']
    200
    >>> d2 = Dict(a=1, b=2, c='3')
    >>> d2.c
    '3'
    >>> d2['empty']
    Traceback (most recent call last):
        ...
    KeyError: 'empty'
    >>> d2.empty
    Traceback (most recent call last):
        ...
    AttributeError: 'Dict' object has no attribute 'empty'
    >>> d3 = Dict(('a', 'b', 'c'), (1, 2, 3))
    >>> d3.a
    1
    >>> d3.b
    2
    >>> d3.c
    3
    """

    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

