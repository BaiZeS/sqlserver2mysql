from mysql_server import *
from Isolated_forests import *


#define default paramer
solid_width = 600
table_names = ['waterhisdata_new', 'specparamscollect_new']


# get mysql sensor name
def mysql_sensor(table_name):
    '''
    初始化需修改
    查询mysql表单内传感器
    table_name:表单名称;
    ->col_data(字段名); data_mysql((char:DeviceID),);
    '''
    sql_mysql_query = r"select DeviceID from " + '%s' % table_name + r" group by DeviceID;"
    col_mysql_query = r"show columns from %s" % table_name

    try:
        mysql_conn = connect_mysql()      # 连接数据库
        col_name = query_mysql(mysql_conn, col_mysql_query)    # 查询字段名
        col_data = [col[0] for col in col_name]
        data_mysql = query_mysql(mysql_conn, sql_mysql_query)   # 执行查询
        close_mysql(mysql_conn)     # 关闭数据库连接
        sensor_name = list(map(lambda x:x[0], data_mysql))
        print('check mysql sensor success~')
    except:
        print('check mysql sensor error')
    return col_data, sensor_name


# get data from mysql
def get_sensor_data(table_name:str, deviceid:str):
    '''
    get sensor data of device in table_name->data:list
    '''
    # test stage:just get one month data from mysql
    if table_name == 'waterhisdata_new':
        sql_query = "select NewID, Water from waterhisdata_new where DeviceID = '%s'" % deviceid
    elif table_name == 'specparamscollect_new':
        sql_query = "select NewID, Dcm_Para8 from specparamscollect_new where DeviceID = '%s'" % deviceid
    else:
        print('error table, please try it later again~')
    sql_conn = connect_mysql()
    sql_res = query_mysql(sql_conn, sql_query)
    close_mysql(sql_conn)

    # change data former
    data_ls = list(map(lambda x:(int(x[0]), float(x[1])), sql_res))
    return data_ls


# target noise in sensor data by solid window
def get_noise(data:list, solid_width:int):
    '''
    use Isolated_forests sign abnormal data->abnormal index:list
    '''
    # define noise list of sensor
    noise_index = []
    # get sliding window
    data_num = len(data)

    for window_index in range(0, data_num, solid_width):
        # slide data and get wind_flag
        data_slide = data[window_index:window_index+solid_width]

        # get noise of window
        ab_index = forests(data_slide)
        noise_window = list(map(lambda x:x+window_index, ab_index))
        # splicing noise in window together
        noise_index.extend(noise_window)

    return noise_index
    

def update_noise(NewID_noise_index:list, table_name:str, sensorID:str):
    '''
    update sensor noise label by NewID
    '''
    # create noise data tuple
    noise_list = list(map(lambda x:(str(x), sensorID), NewID_noise_index))
    # define update sql
    update_sql = 'update %s set abnormal=1 where NewID=%%s and DeviceID=%%s' % table_name
    # update noise label to mysql
    sql_conn = connect_mysql()
    execute_mysql(sql_conn, update_sql, noise_list)
    close_mysql(sql_conn)
    print('更新表 %s 传感器 %s 异常值数量：%s' % (table_name, sensorID, len(noise_list)))


if __name__ == '__main__':
    for name in table_names:
        # get sensor in tables
        column, sensor_name = mysql_sensor(name)

        for sensor in sensor_name:
            # get data set of sensor in every table
            data_set = get_sensor_data(name, sensor)
            # get data value of sensor
            data_values = list(map(lambda x:x[1], data_set))
            # get noise index of data set
            if len(data_values) != 0:
                sensor_noise_index = get_noise(data_values, solid_width)
            # get noise NewID of data set
            NewID_noise_index = [data_set[one][0] for one in sensor_noise_index]
            # print number of noise for every sensor
            print('noise number of %s is %s' % (sensor, str(len(NewID_noise_index))))
            # update noise id if num of noise is not 0
            if len(NewID_noise_index) != 0:
                update_noise(NewID_noise_index, name, sensor)