import pymysql


def connect_mysql():
    # 连接数据库
    try:
        conn = pymysql.connect(
            host='localhost',  # 连接服务器mysql
            user='wateruser',  # 用户名
            passwd='scu@404watershow_yc!?!',  # 密码
            port=3306,  # 端口，默认为3306
            db='wjwatersystem',  # 数据库名称
            charset='utf8',  # 字符编码
        )
    except:
        print('error in connecting to mysql')
    return conn

def close_mysql(conn):
    '''
    关闭数据库
    '''
    conn.close()    # 关闭数据连接


def query_mysql(conn, sql):
    '''
    定义查询:
    conn:数据库对象; sql:查询语句;
    ->data:查询结果; col_data:字段名;
    '''
    cur = conn.cursor()  # 生成游标对象
    cur.execute(sql)  # 执行SQL语句
    data = cur.fetchall()  # 通过fetchall方法获得数据
    cur.close()  # 关闭游标
    return data


def execute_mysql(conn, sql, param=[]):
    '''
    定义执行:
    conn:数据库对象;sql:查询语句;
    '''
    try:
        cur = conn.cursor()  # 生成游标对象
        if len(param) > 0:
            print('executemang mysql replace~')
            cur.executemany(sql, param)  # 执行插入的sql语句,多条数据
        else:
            print('execute mysql insert~')
            cur.execute(sql)        # 执行单挑查询
        conn.commit()  # 提交到数据库执行
    except:
        conn.rollback()  # 如果发生错误则回滚
    finally:
        cur.close() # 关闭游标
