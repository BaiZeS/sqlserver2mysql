import pymssql


def connect_sqlserver():
    # 连接数据库
    try:
        conn = pymssql.connect(
            host='124.71.225.163',
            user='sa',
            password='scwj.123',
            database='wjwatersystem'
        )
    except:
        print('error in connecting to sqlserver')
    return conn


def close_sql(conn):
    conn.close()    # 关闭数据库连接


def query_sql(conn, sql):
    '''
    定义查询:
    conn:数据库对象; sql:查询语句;
    '''
    cursor = conn.cursor()  # 建立游标
    cursor.execute(sql) # 执行查询语句
    data = cursor.fetchall()    # 获取查询结果
    cursor.close()  # 关闭游标
    return data
