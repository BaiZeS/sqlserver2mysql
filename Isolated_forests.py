from sklearn.ensemble import IsolationForest
import numpy as np

def forests(data_origin:list):
    '''
    孤立森林检测异常值, 返回异常值下标
    '''
    # 将传入的一维数据打包成二维
    data_list = list(zip(range(len(data_origin)), data_origin))
    data_train = np.array(data_list)

    # 构建模型
    model = IsolationForest(n_estimators=max(256, int(0.2*len(data_train))), max_samples='auto', contamination=float(0.03), max_features=1.0)

    # 训练模型
    model.fit(data_train)

    # 预测 decision_function 可以得出异常评分
    data_score = model.decision_function(data_train)
    # print('本次得分最大值：%s, 本次得分均值：%s, 本次得分最小值:%s' % (np.amax(data_score), np.mean(data_score), np.amin(data_score)))
    
    # 根据异常得分判断异常值
    score_mean = np.mean(data_score)
    score_min = np.amin(data_score)
    score_std = np.std(data_score)

    if score_min < -0.3:
        # predict() 函数 可以得到模型是否异常的判断，-1为异常，1为正常
        data_res = model.predict(data_train)
        abnormal_index = np.where(data_res==-1)[0]
    else:
        # 得分低于均值3.5倍标准差为异常
        abnormal_index = np.where(data_score<(score_mean-3.5*score_std))[0]
    
    # print(abnormal_index)

    # 返回异常值位置
    return abnormal_index