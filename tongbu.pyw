from logging import exception
import os
import time

from sql_server import *
from mysql_server import *
from sensor_location import update_sensor_info
import datetime


def mysql_sensor(table_name):
    '''
    初始化需修改
    查询mysql表单内传感器
    table_name:表单名称;
    ->col_data(字段名); data_mysql((char:DeviceID, datetime:CollectionTime),);
    '''
    # sql_mysql_query = r"select DeviceID, MIN(CollectionTime) as time from " + '%s' % table_name + r" group by DeviceID order by time asc;" # 初始化表单
    sql_mysql_query = r"select DeviceID, MAX(CollectionTime) as time from " + '%s' % table_name + r" group by DeviceID order by time desc;"
    col_mysql_query = r"show columns from %s" % table_name

    try:
        mysql_conn = connect_mysql()      # 连接数据库
        col_name = query_mysql(mysql_conn, col_mysql_query)    # 查询字段名
        col_data = [col[0] for col in col_name]
        data_mysql = query_mysql(mysql_conn, sql_mysql_query)       # 执行查询
        close_mysql(mysql_conn)     # 关闭数据库连接
        print('check mysql sensor success~')
    except:
        print('check mysql sensor error')
    return col_data, data_mysql


def sql_sensor(table_name):
    '''
    查询sqlserver表单内传感器
    table_name:表单名称;
    ->data_sql((char:DeviceID, datetime:CollectionTime),);
    '''
    sql_mysql_query = r"select DeviceID, MAX(CollectionTime) as time from " + '%s' % table_name + r" group by DeviceID order by time desc;"

    try:
        sql_conn = connect_sqlserver()      # 连接数据库
        data_sql = query_sql(sql_conn, sql_mysql_query)       # 执行查询
        close_mysql(sql_conn)     # 关闭数据库连接
        print('check sql sensor success~')
    except:
        print('check sql sensor error')
    return data_sql


def synchronous_new(table_name, col_data, sensor_data):
    '''
    同步数据表单至新表
    table_name:表单名; col_data:字段名; sensor_data:传感器数据
    '''
    try:
        sql_conn = connect_sqlserver()      # 连接sql数据库
        mysql_conn = connect_mysql()        # 连接MySQL数据库
    except:
        print('connect to sql error, please try it again')

    for sensor_info in sensor_data:
        sensor_name = sensor_info[0]        # 获取各个传感器名称
        collection_time = sensor_info[1]        # 获得MySQL中传感器数据最后更新时间
        # 按时间升序查询
        sql_query = r"select * from %s where deviceid = '%s' and collectiontime > '%s' order by CollectionTime asc" % (table_name[0:-4], sensor_name, collection_time)
        print('sql_query:\t', sql_query)

        try:
            data_sql = query_sql(sql_conn, sql_query)       # 执行查询,获取该传感器最新数据
            # 查询传感器已写入新表的数据量
            count_query_new = r"select count(1) from %s where deviceid = '%s'" % (table_name, sensor_name)
            # print('count sensor_data num:', count_query_new)
            data_count = query_mysql(mysql_conn, count_query_new)[0][0]
            print('len_data:', len(data_sql), 'data_count:', data_count)        # 查询更新数据量

            if data_sql:
                data_len = len(col_data)     # 获取单条字段插入数据个数
                # 添加新表信息
                data_new_sql = update_sensor_info(table_name, data_sql, data_count)
                # print(data_new_sql)
                # 插入MySQL
                replace_mysql = "replace into %s (%s) values " % (table_name, ','.join(col_data)) + "("+(("%"+"s,")*data_len)[0:-1]+")"     # 拼接查询MySQL查询语句
                execute_mysql(mysql_conn, replace_mysql, data_new_sql)     # 执行多条插入语句
                # print(replace_mysql, data_new_sql)
            else:
                print('%s has not update data' % sensor_name)
        except:
            print('%s synchronous_new sensor erro' % sensor_name)
    print('synchronous_new success~')

    try:
        close_sql(sql_conn)     # 关闭sqlserver连接
        close_mysql(mysql_conn)     # 关闭MySQL连接
    except:
        print('close sql error, please check code')


def synchronous(table_name, col_data, sensor_data):
    '''
    同步数据表单
    table_name:表单名; col_data:字段名; sensor_data:传感器数据
    '''
    try:
        sql_conn = connect_sqlserver()      # 连接sql数据库
        mysql_conn = connect_mysql()        # 连接MySQL数据库

        for sensor_info in sensor_data:
            sensor_name = sensor_info[0]        # 获取各个传感器名称
            collection_time = sensor_info[1]        # 获得MySQL中传感器数据最后更新时间
            # 按时间升序查询
            sql_query = r"select * from %s where deviceid = '%s' and collectiontime > '%s' order by CollectionTime asc" % (table_name, sensor_name, collection_time)
            print(sql_query)
            data_sql = query_sql(sql_conn, sql_query)       # 执行查询,获取该传感器最新数据
            print(len(data_sql))        # 查询更新数据量

            if data_sql:
                data_len = len(col_data)     # 获取单条数据长度
                # 插入MySQL
                replace_mysql = "replace into %s (%s) values " % (table_name, ','.join(col_data)) + "("+(("%"+"s,")*data_len)[0:-1]+")"     # 拼接查询MySQL查询语句
                execute_mysql(mysql_conn, replace_mysql, data_sql)     # 执行多条插入语句
            else:
                print('%s has not update data' % sensor_name)
        print('synchronous success~')

        close_sql(sql_conn)     # 关闭sqlserver连接
        close_mysql(mysql_conn)     # 关闭MySQL连接
    except exception as e:
        print('synchronous woring:', e)


if __name__ == '__main__':
    while True:
        datetime_now = datetime.datetime.now()
        try:
            table_name_list = ['waterhisdata', 'rainfalldata', 'specparamscollect']
            for table_name in table_name_list:
                # 获取MySQL各个传感器数据
                col_data, sensors_data = mysql_sensor(table_name)
                # 同步传感器数据
                synchronous(table_name, col_data, sensors_data)
            
            table_new_list = ['waterhisdata_new', 'rainfalldata_new', 'specparamscollect_new']
            for table_new in table_new_list:
                # 获取MySQL新表中各个传感器数据
                # 初始化信息
                # col_data_old, sensors_data_new = mysql_sensor(table_new[0:-4])
                # col_data_new, sensors_data_old = mysql_sensor(table_new)
                # 同步数据至新表
                col_data_new, sensors_data_new = mysql_sensor(table_new)
                # print(table_new, sensors_data_new)
                # 同步传感器数据至新表
                synchronous_new(table_new, col_data_new, sensors_data_new)
            print(datetime_now)
        except Exception as e:
            with open(os.path.join(os.path.dirname(__file__), 'tongbu_sql.log'), 'a+') as f:
                f.write('%s synchronous has not sucess, will try it 5 minutes later \n' % datetime_now)
                f.write('error: %s \n' % repr(e))
                f.close()
        finally:
            time.sleep(5*60)