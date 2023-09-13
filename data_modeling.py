####################################################################建模数据####################################################################
#####################################################################################################################################
'''数据预处理'''
import pandas as pd 

colnames = ['cust_id', 'create_date', 'executed_flag_7', 'executed_flag_15', 'ordered_flag_7', 'ordered_flag_15',
            'cust_level', 'cust_type', 'cust_group_type', 'is_office_weixin', 'rg_time', 'office_weixin_time', 'sex', 'age', 'age_group',
            'access_channel_cnt', 'executed_orders_cnt', 'complaint_orders_cnt', 'cp_avg', 'cp_min', 'cp_max', 'satisfaction_avg',
            'vac_executed_cnt', 'vac_cp_avg', 'vac_satisfaction_avg', 'decision_time', 'browsing_days', 'pv', 'pv_daily_avg',
            'browsing_time', 'browsing_time_max', 'browsing_time_daily_avg', 'browsing_products_cnt', 'browsing_products_cnt_daily_avg',
            'to_booking_pages_cnt', 'search_times', 'vac_search_times', 'lowest_price_avg', 'vac_lowest_price_avg', 
            'browsing_vac_prd_cnt', 'browsing_single_prd_cnt', 'collect_coupons_cnt', 'prd_satisfaction', 'prd_comp_grade_level',
            'prd_remark_amount', 'prd_essence_amount', 'prd_coupon_amount', 'prd_money_amount', 'prd_photo_amount',
            'comm_days', 'comm_freq', 'comm_freq_daily_avg', 'call_pct', 'robot_pct', 'officewx_pct', 'chat_pct', 'channels_cnt',
            'comm_time', 'comm_time_daily_avg', 'call_completing_rate', 'calls_freq', 'calls_freq_daily_avg', 'active_calls_freq',
            'active_calls_pct', 'chats_freq', 'chats_freq_daily_avg', 'active_chats_freq', 'active_chats_pct',
            'vac_mention_num', 'single_mention_num']
order = pd.read_csv("/opt/****/jupyter/****/tmp_data/tmp_cust_order_pred_202307.csv", encoding='utf-8', sep=chr(1), names=colnames, na_values='\\N')
order


data = order.copy()
data['sex'] = data['sex'].map({'男': 1, '女': 0})
data['age_group'] = data['age_group'].map({'0~15岁': 1, '16~25岁': 2, '26~35岁': 3, '36~45岁': 4, '大于46岁': 5})
# 数据类型转换
cate_varlist = ['executed_flag_7', 'executed_flag_15', 'ordered_flag_7', 'ordered_flag_15', 'cust_level', 'cust_type', 
                'cust_group_type', 'is_office_weixin', 'sex', 'age_group', 'prd_comp_grade_level']
num_varlist = ['rg_time', 'office_weixin_time', 'age', 'access_channel_cnt', 'executed_orders_cnt', 'complaint_orders_cnt', 
               'cp_avg', 'cp_min', 'cp_max', 'satisfaction_avg', 'vac_executed_cnt', 'vac_cp_avg', 'vac_satisfaction_avg', 
               'decision_time', 'browsing_days', 'pv', 'pv_daily_avg', 'browsing_time', 'browsing_time_max', 'browsing_time_daily_avg', 
               'browsing_products_cnt', 'browsing_products_cnt_daily_avg', 'to_booking_pages_cnt', 'search_times', 'vac_search_times', 
               'lowest_price_avg', 'vac_lowest_price_avg', 'browsing_vac_prd_cnt', 'browsing_single_prd_cnt', 'collect_coupons_cnt', 
               'prd_satisfaction', 'prd_remark_amount', 'prd_essence_amount', 'prd_coupon_amount', 'prd_money_amount', 
               'prd_photo_amount', 'comm_days', 'comm_freq', 'comm_freq_daily_avg', 'call_pct', 'robot_pct', 'officewx_pct', 
               'chat_pct', 'channels_cnt', 'comm_time', 'comm_time_daily_avg', 'call_completing_rate', 'calls_freq', 
               'calls_freq_daily_avg', 'active_calls_freq', 'active_calls_pct', 'chats_freq', 'chats_freq_daily_avg', 
               'active_chats_freq', 'active_chats_pct', 'vac_mention_num', 'single_mention_num']
data[cate_varlist] = data[cate_varlist].astype('category')
# 缺失值填充
data['age'] = data['age'].fillna(data['age'].mean())
data['satisfaction_avg'] = data['satisfaction_avg'].fillna(data['satisfaction_avg'].mean()) 
data['vac_satisfaction_avg'] = data['vac_satisfaction_avg'].fillna(data['vac_satisfaction_avg'].mean()) 
data['lowest_price_avg'] = data['lowest_price_avg'].fillna(data['lowest_price_avg'].mean()) 
data['vac_lowest_price_avg'] = data['vac_lowest_price_avg'].fillna(data['vac_lowest_price_avg'].mean()) 
data['prd_satisfaction'] = data['prd_satisfaction'].fillna(data['prd_satisfaction'].mean())
# 对类别变量进行独热编码
encoded_data = pd.get_dummies(data[cate_varlist[4:]], prefix=None, drop_first=True).astype(int)
# 数据标准化
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
data[num_varlist] = scaler.fit_transform(data[num_varlist])
# 将编码后的数据与原始数据合并
data = pd.concat([data.iloc[:,:6], encoded_data, data[num_varlist]], axis=1)
data.info()


#####################################################################################################################################
'''数据重采样'''
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline
from collections import Counter

print(f'Original dataset shape %s' % Counter(y))
pipeline = Pipeline([('over', SMOTE(sampling_strategy=0.2, k_neighbors=5)),
                     ('under', RandomUnderSampler(sampling_strategy=0.6))])
# 重采样之后0：1 = 5:3
X, y = pipeline.fit_resample(X, y)
print('Resampled dataset shape %s' % Counter(y))


#####################################################################################################################################
'''随机森林建模'''
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import joblib

X = data.iloc[:,6:]
y = data['executed_flag_7']
# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 训练随机森林模型
rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
rf_classifier.fit(X_train, y_train)

# 在测试集上进行预测
y_pred = rf_classifier.predict(X_test)  # 预测类别
# y_pred = rf_classifier.predict_proba(X_test)  # 预测概率估计

# 计算模型评价指标
print("AUC：", round(roc_auc_score(y_test, y_pred), 4))
print(classification_report(y_test, y_pred))
print("混淆矩阵：", confusion_matrix(y_test, y_pred))

# 保存模型
joblib.dump(rf_classifier, './rf_classifier_model.joblib')


# 特征重要性可视化
import numpy as np
import matplotlib.pyplot as plt

# 获取特征重要性
feature_importances = rf_classifier.feature_importances_
# 对特征重要性进行排序
sorted_indices = np.argsort(feature_importances)[::-1]  # 降序排列
# 可视化特征重要性
plt.figure(figsize=(12, 6))
plt.bar(range(X_train.shape[1]), feature_importances[sorted_indices])
plt.xticks(range(X_train.shape[1]), X_train.columns[sorted_indices], rotation=90)
plt.xlabel('Feature')
plt.ylabel('Importance')
plt.title('Feature Importance')
plt.tight_layout()
plt.show()


#####################################################################################################################################





####################################################################测试数据####################################################################
'''读取数据'''
colnames = ['cust_id', 'create_date', 'executed_flag_7', 'executed_flag_15', 'ordered_flag_7', 'ordered_flag_15',
            'cust_level', 'cust_type', 'cust_group_type', 'is_office_weixin', 'rg_time', 'office_weixin_time', 'sex', 'age', 'age_group',
            'access_channel_cnt', 'executed_orders_cnt', 'complaint_orders_cnt', 'cp_avg', 'cp_min', 'cp_max', 'satisfaction_avg',
            'vac_executed_cnt', 'vac_cp_avg', 'vac_satisfaction_avg', 'decision_time', 'browsing_days', 'pv', 'pv_daily_avg',
            'browsing_time', 'browsing_time_max', 'browsing_time_daily_avg', 'browsing_products_cnt', 'browsing_products_cnt_daily_avg',
            'to_booking_pages_cnt', 'search_times', 'vac_search_times', 'lowest_price_avg', 'vac_lowest_price_avg', 
            'browsing_vac_prd_cnt', 'browsing_single_prd_cnt', 'collect_coupons_cnt', 'prd_satisfaction', 'prd_comp_grade_level',
            'prd_remark_amount', 'prd_essence_amount', 'prd_coupon_amount', 'prd_money_amount', 'prd_photo_amount',
            'comm_days', 'comm_freq', 'comm_freq_daily_avg', 'call_pct', 'robot_pct', 'officewx_pct', 'chat_pct', 'channels_cnt',
            'comm_time', 'comm_time_daily_avg', 'call_completing_rate', 'calls_freq', 'calls_freq_daily_avg', 'active_calls_freq',
            'active_calls_pct', 'chats_freq', 'chats_freq_daily_avg', 'active_chats_freq', 'active_chats_pct',
            'vac_mention_num', 'single_mention_num']
order_test = pd.read_csv("/opt/****/jupyter/****/tmp_data/tmp_cust_order_pred_20230802.csv", encoding='utf-8', sep=chr(1), names=colnames, na_values='\\N')
order_test



'''数据预处理'''
data_test = order.copy()
data_test['sex'] = data_test['sex'].map({'男': 1, '女': 0})
data_test['age_group'] = data_test['age_group'].map({'0~15岁': 1, '16~25岁': 2, '26~35岁': 3, '36~45岁': 4, '大于46岁': 5})
# 数据类型转换
cate_varlist = ['executed_flag_7', 'executed_flag_15', 'ordered_flag_7', 'ordered_flag_15', 'cust_level', 'cust_type', 
                'cust_group_type', 'is_office_weixin', 'sex', 'age_group', 'prd_comp_grade_level']
num_varlist = ['rg_time', 'office_weixin_time', 'age', 'access_channel_cnt', 'executed_orders_cnt', 'complaint_orders_cnt', 
               'cp_avg', 'cp_min', 'cp_max', 'satisfaction_avg', 'vac_executed_cnt', 'vac_cp_avg', 'vac_satisfaction_avg', 
               'decision_time', 'browsing_days', 'pv', 'pv_daily_avg', 'browsing_time', 'browsing_time_max', 'browsing_time_daily_avg', 
               'browsing_products_cnt', 'browsing_products_cnt_daily_avg', 'to_booking_pages_cnt', 'search_times', 'vac_search_times', 
               'lowest_price_avg', 'vac_lowest_price_avg', 'browsing_vac_prd_cnt', 'browsing_single_prd_cnt', 'collect_coupons_cnt', 
               'prd_satisfaction', 'prd_remark_amount', 'prd_essence_amount', 'prd_coupon_amount', 'prd_money_amount', 
               'prd_photo_amount', 'comm_days', 'comm_freq', 'comm_freq_daily_avg', 'call_pct', 'robot_pct', 'officewx_pct', 
               'chat_pct', 'channels_cnt', 'comm_time', 'comm_time_daily_avg', 'call_completing_rate', 'calls_freq', 
               'calls_freq_daily_avg', 'active_calls_freq', 'active_calls_pct', 'chats_freq', 'chats_freq_daily_avg', 
               'active_chats_freq', 'active_chats_pct', 'vac_mention_num', 'single_mention_num']
data_test[cate_varlist] = data_test[cate_varlist].astype('category')
# 缺失值填充
data_test['age'] = data_test['age'].fillna(data_test['age'].mean())
data_test['satisfaction_avg'] = data_test['satisfaction_avg'].fillna(data_test['satisfaction_avg'].mean()) 
data_test['vac_satisfaction_avg'] = data_test['vac_satisfaction_avg'].fillna(data_test['vac_satisfaction_avg'].mean()) 
data_test['lowest_price_avg'] = data_test['lowest_price_avg'].fillna(data_test['lowest_price_avg'].mean()) 
data_test['vac_lowest_price_avg'] = data_test['vac_lowest_price_avg'].fillna(data_test['vac_lowest_price_avg'].mean()) 
data_test['prd_satisfaction'] = data_test['prd_satisfaction'].fillna(data_test['prd_satisfaction'].mean())
# 对类别变量进行独热编码
encoded_data_test = pd.get_dummies(data_test[cate_varlist[4:]], prefix=None, drop_first=True).astype(int)
# 数据标准化
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
data_test[num_varlist] = scaler.fit_transform(data_test[num_varlist])
# 将编码后的数据与原始数据合并
data_test = pd.concat([data_test.iloc[:,:6], encoded_data_test, data_test[num_varlist]], axis=1)
data_test.info()



'''模型测试'''
X_t = data_test.iloc[:,6:]
y_t = data_test['executed_flag_7']
y_t_pred = rf_classifier.predict(X_t)  # 预测类别
# 计算模型评价指标
print("AUC：", round(roc_auc_score(y_t, y_t_pred), 4))
print(classification_report(y_t, y_t_pred))
print("混淆矩阵：", confusion_matrix(y_t, y_t_pred))


