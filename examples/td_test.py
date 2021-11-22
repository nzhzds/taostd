import logging
import pandas as pd
from taostd import td
from datetime import datetime, timedelta


def create_stable():
    """创建stable"""
    td.execute("CREATE STABLE IF NOT EXISTS meters(ts timestamp, current float, voltage float, phase int) TAGS(location nchar(20), groupId tinyint)")


def test_insert_one_with_stable():
    """往单表插入一条数据，如果表不存在就先创建表，必须指定tags，如本例中的'location'和'groupId'"""
    td.insert_one_with_stable(table='meter_01', stable='meters', ts=datetime.now()-timedelta(hours=1), current=0.2550, voltage=0.3542, phase=0, location='北京', groupId=0)


def test_insert_one():
    """往单表插入一条数据，表已经存在, 可以直接插入，不需要指定 stable和tags"""
    td.insert_one(table='meter_01', ts=datetime.now()-timedelta(minutes=10), current=0.3550, voltage=0.5542, phase=1)


def test_insert_many():
    """往单张表插入多条记录"""
    meters = [
        {"ts": datetime.now() - timedelta(minutes=1), "current": 0.3550, "voltage": 0.5542, "phase": 2},
        {"ts": datetime.now() + timedelta(hours=1), "current": 0.3550, "voltage": 0.5542, "phase": 3},
    ]
    td.insert_many(table='meter_01', args=meters)


def test_insert_many_with_stable():
    """往单张表插入多条记录， 如果表不存在，就创建表"""
    meters = [
        {"ts": "2021-11-19 15:30:44.123445", "current": 0.3550, "voltage": 0.5542, "phase": 0, "location": "上海", "groupId": 1},
        {"ts": datetime.now() - timedelta(hours=1), "current": 0.3550, "voltage": 0.5542, "phase": 1, "location": "上海", "groupId": 1},
    ]
    td.insert_many_with_stable(table='meter_02', stable="meters", args=meters)


def insert_many_tables():
    """同时往多张表插入记录, 'meter_02'表已经存在，可以不加stable；'meter_03'必须指定stable"""
    meters = [
        {"table": "meter_01", "ts": "2021-11-19 17:30:43.1234", "current": 0.3550, "voltage": 0.5542, "phase": 4, "location": "北京", "groupId": 0},
        {"table": "meter_02", "ts": "2021-11-19 17:30:43.1234", "current": 0.3550, "voltage": 0.5542, "phase": 2, "location": "上海", "groupId": 1},
        {"table": "meter_03", "stable": "meters", "ts": datetime.now(), "current": 0.3550, "voltage": 0.5542, "phase": 0, "location": "天津", "groupId": 2},
    ]
    td.insert_many_tables(args=meters)


@td.with_connection
def drop_table():
    """删除表, 加上@td.with_connection整个方法里只需要获取一次数据库连接，多条执行语句共用一个connection"""
    td.drop_table("meter_01")
    td.drop_table("meter_02")
    td.drop_table("meter_03")
    td.execute("DROP STABLE IF EXISTS meters")


if __name__ == '__main__':
    # 设置logging等级为DEBUG，可以打印执行的SQL
    logging.basicConfig(level=logging.INFO)

    # 使用前，请在taos客户端创建数据库：CREATE DATABASE IF NOT EXISTS test;
    # 必须指定database；pool_size指定连接池大小，是可选的，默认是1，可以根据需要设置大小
    # tz_offset默认是东八区：8
    # 本例使用客户端taos.cfg配置，其它参数请参照taos.connect()方法，初始化后，项目里到处都可以用了
    td.init_db(database="test", pool_size=2, tz_offset=8)

    # 创建stable
    create_stable()

    # 往单表插入一条数据，如果表不存在就先创建表
    test_insert_one_with_stable()
    assert td.get("select count(1) from meter_01") == 1

    # 往单表插入一条数据，表已经存在, 可以直接插入，不需要指定 stable和tags
    test_insert_one()
    assert td.get("select count(1) from meter_01") == 2

    # 往单张表插入多条记录
    test_insert_many()
    assert td.get("select count(1) from meter_01") == 4

    # 往单张表插入多条记录， 如果表不存在，就创建表
    test_insert_many_with_stable()
    assert td.get("select count(1) from meter_02") == 2

    # 同时往多张表插入记录, 'meter_02'表已经存在，可以不加stable；'meter_03'必须指定stable
    insert_many_tables()
    assert td.get("select count(1) from meter_01") == 5
    assert td.get("select count(1) from meter_02") == 3
    assert td.get("select count(1) from meter_03") == 1

    print("last_row:", td.select_one("select last_row(*) from meters"))
    print("--------------------------------------------------")
    meter_list = td.select("select * from meters")
    # 转换成pandas的DataFrame
    df = pd.DataFrame(meter_list)
    print(df.head())

    # 删除表
    drop_table()
