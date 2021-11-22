import taos
import logging
import functools
from taostd import cache
from datetime import datetime, timedelta
from taostd.model import TDCtx, TDError, Engine, ConnectionCtx, Dict, MultiColumnsError
from taostd.bind import bind_params, batch_bind_params
from taostd.sql import get_sql_tags, get_sql_values, get_insert_sql
from queue import Queue
import copy

# connection pool
_pool = None
# thread-local context:
_db_ctx = None
_tz_offset = None


def init_db(database, pool_size=1, tz_offset=8, *args, **kwargs):
    global _pool
    global _db_ctx
    global _tz_offset
    _tz_offset = tz_offset
    if _pool is not None:
        raise TDError('DB is already initialized.')

    kwargs['database'] = database
    engine = Engine(lambda: taos.connect(*args, **kwargs))
    _pool = Queue(maxsize=pool_size)
    for _ in range(pool_size):
        _pool.put_nowait(engine.connect())
    _db_ctx = TDCtx(_pool)
    # examples connection...
    logging.info('Init TDengine engine <%s> ok.' % hex(id(engine)))

    _init_table_cache()


def connection():
    """
    Return _ConnectionCtx object that can be used by 'with' statement:
    with connection():
        pass
    """
    global _db_ctx
    return ConnectionCtx(_db_ctx)


def with_connection(func):
    """
    Decorator for reuse connection.
    @with_connection
    def foo(*args, **kw):
        f1()
        f2()
        f3()
    """
    global _db_ctx

    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with ConnectionCtx(_db_ctx):
            return func(*args, **kw)

    return _wrapper


@with_connection
def get(sql: str):
    """ execute select SQL and return unique result.
        select count(1) form meters
        or
        select lass(ts) from meters where tag = 'xxx'
        :return: only value
    """
    result = _query(sql)
    try:
        value = result.next()
    except StopIteration:
        return None
    except taos.error.OperationalError:
        return None
    if len(value) == 1:
        return value[0]
    else:
        raise MultiColumnsError('Expect only one column.')


@with_connection
def select_one(sql: str):
    """ execute select SQL and return unique result.
        select last_row(*) from meters where tag = 'xxx';
        :return: {k:v}
    """
    result = _select(sql)
    if result:
        return result[0]
    else:
        return None


@with_connection
def select(sql: str):
    """ execute select SQL and return list results."""
    return _select(sql)


@with_connection
def execute(sql: str, params=None) -> int:
    """
    simple execute SQL and return affected rows.
    params is not None, execute insert sql by prepare statement. like: insert into meters(?,?,?);
    :param sql: SQL
    :param params: parameters by taos.new_bind_params() function created
    :return: affected rows: int
    """
    logging.debug('SQL: %s' % sql)
    if params:
        return _stmt_execute(sql, params)
    else:
        return _cursor_execute(sql)


@with_connection
def insert_one(table: str, **kwargs):
    """
    insert one row into the exit table.
    :param table: table name
    :param kwargs: values: {field: value}, tags are not necessary
    :return: affected_rows
    """
    cache_stable = _get_table_cache(table)
    if cache_stable is None:
        raise TDError(f"Table '{table}' does not exist，please use 'insert_one_with_stable' function instead.")

    return _insert_one(table, cache_stable, **kwargs)


@with_connection
def insert_one_with_stable(table: str, stable: str, **kwargs):
    """
    insert one row into table, and create it if the table does exit.
    :param table: table name
    :param stable: stable name
    :param kwargs: values: {field: value}, tags are not necessary
    :return: affected_rows
    """
    global _tz_offset
    cache_stable = _get_table_cache(table)
    if cache_stable is None:
        desc = _get_stable_cache(stable)
        num_columns = desc['columns']
        params = bind_params(num_columns, _tz_offset, desc['fields'], **kwargs)
        return _insert_one_with_stable(table, stable, num_columns, desc, params, **kwargs)
    elif cache_stable != stable:
        raise TDError(f"expect stable '{cache_stable}'，but input '{stable}'.")
    else:
        return _insert_one(table, cache_stable, **kwargs)


@with_connection
def insert_many(table: str, args: list, batch_size=100):
    """
    insert many rows into the exit table.
    :param table:
    :param args: [{"ts": xxx, ...}, {}]
    :param batch_size:
    :return: affected_rows
    """
    cache_stable = _get_table_cache(table)
    if cache_stable is None:
        raise TDError(f"Table '{table}' does not exist ，please use 'insert_many_with_stable' function instead.")

    return _insert_many(table, cache_stable, args, batch_size=batch_size)


@with_connection
def insert_many_with_stable(table: str, stable: str, args: list, batch_size=100):
    """
    insert many lows into table, and create it if the table does exit.
    :param table: table name
    :param stable: stable name
    :param args: [{"ts": xxx, ...}, {}]
    :param batch_size:
    :return: affected_rows
    """
    cache_stable = _get_table_cache(table)
    if cache_stable is None:  # table未创建，需要点指定stable
        desc = _get_stable_cache(stable)
        num_columns = desc['columns']
        tags = [f for f in desc['fields'] if f['Note'] == "TAG"]
        tag_sql_values = get_sql_tags(tags, **(args[0]))
        sql = f"INSERT INTO {table} USING {stable} TAGS({tag_sql_values}) VALUES ({','.join(['?' for i in range(num_columns)])})"
        logging.debug('SQL: %s' % sql)

        if args:
            affected_rows = 0
            length = len(args)
            if length <= batch_size:
                affected_rows = _batch_stmt_execute(sql, num_columns, desc['fields'], args)
            else:
                affected_rows = 0
                batches = length // batch_size + 1
                for i in range(batches):
                    start = i * batch_size
                    end = start + batch_size
                    try:
                        affected_rows += _batch_stmt_execute(sql, num_columns, desc['fields'], args[start:end])
                    except:
                        logging.error(f"execute {i} batch error.")
            cache.set(table, stable)
            return affected_rows
        else:
            return 0
    elif cache_stable != stable:
        raise TDError(f"expect stable '{cache_stable}'，but input '{stable}'.")
    else:  # table已创建， 直接插入VALUES
        return _insert_many(table, stable, args)


@with_connection
def insert_many_tables(args: list, batch_size=100):
    """
    insert many lows into each table, and create it if the table does exit.
    :param args: [{"table": xxx, ["stable": xxx,] “ts”: "2021-01-01 00:22:44.000"...}, {}]
    :param batch_size:
    :return: affected_rows
    """
    if args:
        length = len(args)
        if length <= batch_size:
            return _insert_many_tables(args)
        else:
            affected_rows = 0
            batches = length // batch_size + 1
            for i in range(batches):
                start = i * batch_size
                end = start + batch_size
                try:
                    affected_rows += _insert_many_tables(args[start:end])
                except:
                    logging.error(f"execute {i} batch error.")
            return affected_rows
    else:
        return 0


@with_connection
def drop_table(table: str):
    _cursor_execute(f"DROP TABLE IF EXISTS {table}")
    cache.delete(table)


@with_connection
def _init_table_cache():
    logging.info("Init table meta cache")
    stables = _select("show stables")
    for stbl in stables:
        fields = _select(f"describe {stbl['name']}")
        fields = [_del_field_length(f) for f in fields]
        cache.set(stbl['name'], {'columns': stbl['columns'], 'tags': stbl['tags'], 'fields': fields})

    tables = _select("show tables")
    for tbl in tables:
        cache.set(tbl['table_name'], tbl['stable_name'])


def _query(sql: str):
    global _db_ctx
    logging.debug('SQL: %s' % sql)
    return _db_ctx.connection.query(sql)


def _stmt_execute(sql: str, args):
    """
    :return: TaosResult
    """
    global _db_ctx
    stmt = None
    try:
        stmt = _db_ctx.statement(sql)
        if isinstance(args, list):
            for arg in args:
                stmt.bind_param(arg)
        else:
            stmt.bind_param(args)

        stmt.execute()
        return stmt.use_result()
    finally:
        if stmt:
            stmt.close()


def _cursor_execute(sql: str):
    global _db_ctx
    cursor = None
    try:
        cursor = _db_ctx.cursor()
        return cursor.execute(sql)
    finally:
        if cursor:
            cursor.close()


def _get_table_cache(table: str):
    stable = cache.get(table)
    if stable:
        return stable

    tables = _select(f"show tables like '{table}'")
    for tbl in tables:
        if tbl == table:
            stable = tbl['stable_name']
            cache.set(tbl['table_name'], tbl['stable_name'])
            return stable

    return None


def _del_field_length(f):
    del f['Length']
    return f


def _get_stable_cache(stable: str):
    desc = cache.get(stable)
    if desc:
        return desc

    stables = _select(f"show stables like '{stable}'")
    for stbl in stables:
        print(stbl, stable)
        if stbl['name'] == stable:
            fields = _select(f"describe {stable}")
            fields = [_del_field_length(f) for f in fields]
            desc = {'columns': stbl['columns'], 'tags': stbl['tags'], 'fields': fields}
            cache.set(stable, desc)
            return desc

    raise TDError(f"Stable' {stable}' does not exist.")


def _select(sql: str):
    """ execute select SQL and return unique result or list results."""
    result = _query(sql)
    fields = [field['name'] for field in result.fields]
    return [Dict(fields, x) for x in result]


def _insert_one(table: str, stable: str, **kwargs):
    """
    :param table: table name
    :param stable: stable name
    :param kwargs: values: {field: value}, tags are not necessary
    :return: affected_rows
    """
    global _tz_offset
    desc = _get_stable_cache(stable)
    num_columns = desc['columns']
    params = bind_params(num_columns, _tz_offset, desc['fields'], **kwargs)
    sql = get_insert_sql(table, num_columns)
    logging.debug('SQL: %s' % sql)
    try:
        return _stmt_execute(sql, params).affected_rows
    except taos.error.StatementError as err:
        logging.warning(f"'{table}' {err.msg}")
        if err.errno == -2147482782:
            return _insert_one_with_stable(table, stable, num_columns, desc, params, **kwargs)
        else:
            raise err


def _insert_one_with_stable(table, stable, num_columns, desc, params, **kwargs):
    tags = [f for f in desc['fields'] if f['Note'] == "TAG"]
    tag_sql_values = get_sql_tags(tags, **kwargs)
    sql = f"INSERT INTO {table} USING {stable} TAGS({tag_sql_values}) VALUES ({','.join(['?' for i in range(num_columns)])})"
    logging.debug('SQL: %s' % sql)
    result = _stmt_execute(sql, params)
    cache.set(table, stable)
    return result.affected_rows


def _batch_stmt_execute(sql, num_columns, fields, args: list):
    global _tz_offset
    params = batch_bind_params(num_columns, _tz_offset, fields, args)
    return _stmt_execute(sql, params).affected_rows


def _insert_many_with_stable(table: str, stable: str, num_columns, desc, args: list):
    tags = [f for f in desc['fields'] if f['Note'] == "TAG"]
    tag_sql_values = get_sql_tags(tags, **(args[0]))
    sql = f"INSERT INTO {table} USING {stable} TAGS({tag_sql_values}) VALUES ({','.join(['?' for i in range(num_columns)])})"
    logging.debug('SQL: %s' % sql)
    return _batch_stmt_execute(sql, num_columns, desc['fields'], args)


def _insert_many(table: str, stable: str, args: list, batch_size=1000):
    global _tz_offset
    desc = _get_stable_cache(stable)
    num_columns = desc['columns']
    sql = get_insert_sql(table, num_columns)
    logging.debug('SQL: %s' % sql)

    if args:
        length = len(args)
        if length <= batch_size:
            try:
                return _batch_stmt_execute(sql, num_columns, desc['fields'], args)
            except taos.error.StatementError as err:
                logging.warning(f"'{table}' {err.msg}")
                if err.errno == -2147482782:
                    return _insert_many_with_stable(table, stable, num_columns, desc, args)
                else:
                    raise err
        else:
            affected_rows = 0
            batches = length // batch_size + 1
            for i in range(batches):
                start = i * batch_size
                end = start + batch_size
                try:
                    affected_rows += _batch_stmt_execute(sql, num_columns, desc['fields'], args[start:end])
                except taos.error.StatementError as err:
                    logging.warning(f"'{table}' {err.msg}")
                    if err.errno == -2147482782:
                        affected_rows += _insert_many_with_stable(table, stable, num_columns, desc, args)
            return affected_rows
    else:
        return 0


def _get_sql_with_stable(table, stable, arg):
    desc = _get_stable_cache(stable)
    tags = [f for f in desc['fields'] if f['Note'] == "TAG"]
    tag_sql_values = get_sql_tags(tags, **arg)
    fields = [f for f in desc['fields'] if f['Note'] == ""]
    sql_values = get_sql_values(fields, arg)
    return f"{table} USING {stable} TAGS({tag_sql_values}) VALUES ({sql_values})"


def _insert_many_tables_with_stable(args: list):
    sql_list = ["INSERT INTO"]
    for arg in args:
        table = arg.get("table")
        stable = arg.get("stable")

        if table is None:
            raise TDError("'table' is expected.")

        del arg["table"]
        cache_stable = _get_table_cache(table)
        if cache_stable:
            sql_list.append(_get_sql_with_stable(table, cache_stable, arg))
            if stable:
                del arg["stable"]
        elif stable:
            del arg["stable"]
            sql_list.append(_get_sql_with_stable(table, cache_stable, arg))
        else:
            raise TDError(f"Table '{table}' does not exits, please add stable")

    sql = ' '.join(sql_list)
    logging.debug('SQL: %s' % sql)
    return _cursor_execute(sql)


def _insert_many_tables(args: list):
    args_cp = copy.deepcopy(args)
    sql_list = ["INSERT INTO"]
    for arg in args:
        table = arg.get("table")
        stable = arg.get("stable")

        if table is None:
            raise TDError("'table' is expected.")

        del arg["table"]
        cache_stable = _get_table_cache(table)
        if cache_stable is None:  # table未创建，需要点指定stable
            if stable:
                del arg["stable"]
                sql_list.append(_get_sql_with_stable(table, stable, arg))
            else:
                raise TDError(f"Table '{table}' does not exits, please add stable")
        else:  # table已创建， 直接插入VALUES
            if stable:
                del arg["stable"]
                if cache_stable != stable:
                    raise TDError(f"expect stable '{cache_stable}'，but input '{stable}'.")

            desc = _get_stable_cache(cache_stable)
            fields = [f for f in desc['fields'] if f['Note'] == ""]
            sql_values = get_sql_values(fields, arg)
            sql_list.append(f"{table} VALUES ({sql_values})")

    sql = ' '.join(sql_list)
    logging.debug('SQL: %s' % sql)
    try:
        return _cursor_execute(sql)
    except taos.error.ProgrammingError as err:
        logging.warning(f"'{table}' {err.msg}")
        if err.msg == "Table does not exist":
            return _insert_many_tables_with_stable(args_cp)
        else:
            raise err


def local_datetime(date_time: datetime, zone_hours=8):
    return date_time - timedelta(hours=zone_hours)

