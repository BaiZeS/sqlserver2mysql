location_dic = {
    'GALX000001': '甘洛县普昌镇斯普村',
    'GALX000002': '甘洛县普昌镇马拉哈村（村委会）',
    'GALX000003': '甘洛县阿尔乡马达村沙场旁',
    'GALX000004': '甘洛县阿尔乡马达村（村委会）',
    'GALX000005': '甘洛县日坡沟1号',
    'GALX000006': '甘洛县日坡沟2号',
    'GALX000007': '甘洛县日坡沟3号',
    'WENC000001': '汶川县牛塘沟上',
    'WENC000002': '汶川县牛塘沟下',
    'WENC000003': '汶川县三江汇合',
    'WENC000004': '汶川县西河上',
    'WENC000005': '汶川县西河下',
    'WENC000006': '汶川县中河',
    'SDQG000006': '茂县三道桥沟隧道口',
    'SDQG000005': '茂县三道桥沟山坡上',
    'SDQG000003': '茂县三道桥沟河道边上',
}

def update_sensor_info(table_name, data_sql, data_count, abnormal_index=[]):
    '''
    更新NewID和传感器位置信息至新表中, 更新异常信息
    table_name:表名, data_sql:该类传感器已经存入的数据, data_count:该类传感器已存入数据量, abnormal_index:异常值下标;
    '''
    data_new_sql = []
    for data_one in data_sql:
        data_count = data_count + 1     # 更新计数

        # 获取deviceid
        if table_name == 'waterhisdata_new':
            deviceid = data_one[1][0:10]
        else:
            deviceid = data_one[0][0:10]
        data_new = list(data_one)       # 转换数据结构tuple->list

        # 插入异常值位置信息
        if len(abnormal_index) != 0:
            data_index = data_sql.index(data_one)       # 获取当前数据位置
            if data_index in abnormal_index:
                # 插入异常标志
                data_new.append('1')
            else:
                # 插入默认空值
                data_new.append(None)
        else:
            data_new.append(None)

        # 插入传感器位置
        if deviceid in location_dic.keys():
            # print('device location:', location_dic[deviceid])
            data_new.insert(0, location_dic[deviceid])     
        else:
            # print('device location:', '无位置信息')
            data_new.insert(0, '无位置信息')

        data_new.insert(0, data_count)      # 插入传感器数据计数

        data_new_tuple = tuple(data_new)    # 转换数据结构list->tuple
        data_new_sql.append(data_new_tuple)
    
    # # 输出更新后首条异常数据
    # if len(abnormal_index) != 0:
    #     print('data_new updated info:', data_new_sql[abnormal_index[0]])

    return data_new_sql