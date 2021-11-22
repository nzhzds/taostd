from taostd.model import TDError
from taostd import cache
from datetime import datetime, timedelta


def get_sql_tags(tags, **kwargs):
    have_tag = False
    tag_values = []
    kwargs = {k.lower(): v for k, v in kwargs.items()}
    for tag in tags:
        tag_name = tag['Field']
        param = kwargs.get(tag_name)
        if param is None:
            tag_values.append("NULL")
        else:
            have_tag = True
            field_type = tag['Type']
            if field_type == 'BINARY' or field_type == 'NCHAR':
                tag_values.append(f"'{param}'")
            else:
                tag_values.append(f"{param}")

    if not have_tag:
        raise TDError(f"参数中没有包含tag，tag字段有：{','.join([tag['Field'] for tag in tags])}")

    return ','.join(tag_values)


def get_sql_values(fields, arg):
    values = []
    arg = {k.lower(): v for k, v in arg.items()}
    for f in fields:
        param = arg.get(f['Field'])
        field_type = f['Type']
        if param is None:
            values.append("NULL")
        elif field_type == 'BINARY' or field_type == 'NCHAR' or field_type == 'TIMESTAMP':
            values.append(f"'{param}'")
        else:
            values.append(f"{param}")

    return ','.join(values)


def get_insert_sql(table: str, num_columns: int):
    key = table + "_insert"
    sql = cache.get(key)
    if sql:
        return sql

    sql = f"INSERT INTO {table} VALUES ({','.join(['?' for _ in range(num_columns)])})"
    cache.set(key, sql)
    return sql

