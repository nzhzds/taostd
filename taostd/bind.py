import taos
from datetime import datetime, timezone, timedelta


def bind_params(num_columns: int, tz_offset, fields: list, **kwargs):
    params = taos.new_bind_params(num_columns)
    i = 0
    for f in fields:
        if f['Note'] == "":
            param = kwargs.get(f['Field'])
            if param is None:
                params[i].null()
            elif f['Type'] == 'TIMESTAMP':
                if isinstance(param, str):
                    param = str2time(param)
                param -= timedelta(hours=tz_offset)
                params[i].timestamp(param)
            elif f['Type'] == 'INT':
                params[i].int(param)
            elif f['Type'] == 'INT UNSIGNED':
                params[i].int_unsigned(param)
            elif f['Type'] == 'BIGINT':
                params[i].bigint(param)
            elif f['Type'] == 'BIGINT UNSIGNED':
                params[i].bigint_unsigned(param)
            elif f['Type'] == 'FLOAT':
                params[i].float(param)
            elif f['Type'] == 'DOUBLE':
                params[i].double(param)
            elif f['Type'] == 'BINARY':
                params[i].binary(param)
            elif f['Type'] == 'SMALLINT':
                params[i].smallint(param)
            elif f['Type'] == 'SMALLINT UNSIGNED':
                params[i].smallint_unsigned(param)
            elif f['Type'] == 'TINYINT':
                params[i].tinyint(param)
            elif f['Type'] == 'TINYINT UNSIGNED':
                params[i].tinyint_unsigned(param)
            elif f['Type'] == 'BOOL':
                params[i].bool(param)
            elif f['Type'] == 'NCHAR':
                params[i].nchar(param)

            i += 1
    return params


def batch_bind_params(num_columns: int, tz_offset, fields: list, args: list):
    params = []
    for arg in args:
        params.append(bind_params(num_columns, tz_offset, fields, **arg))
    return params


def str2time(t_str: str):
    str_len = len(t_str)
    if str_len == 10:
        return datetime.strptime(t_str, "%Y-%m-%d")
    elif str_len == 13:
        return datetime.strptime(t_str, "%Y-%m-%d %H")
    elif str_len == 16:
        return datetime.strptime(t_str, "%Y-%m-%d %H:%M")
    elif str_len == 19:
        return datetime.strptime(t_str, "%Y-%m-%d %H:%M:%S")
    elif 21 <= str_len <= 26:
        return datetime.strptime(t_str, "%Y-%m-%d %H:%M:%S.%f")
    else:
        raise KeyError("Invalid time format.")