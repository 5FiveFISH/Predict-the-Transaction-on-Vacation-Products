# <font color = 'green' >度假产品成交预测</font>
<br>

## <font color = 'blue' >一、项目描述</font>

### **1. 背景描述**  
&emsp;&emsp;在竞争激烈的旅游市场中，了解用户行为并准确预测未来的成交趋势对于成功运营旅游平台至关重要。本项目是在项目【[7月份用户行为分析](https://github.com/5FiveFISH/Behavior-Analysis-of-Users-who-Ordered-in-July.git)】基础上进行的优化，旨在解决一系列挑战，包括特征提取、数据不平衡和预测时间范围等问题。现实中，在提高业务利润的需求下，旅游平台更关注度假产品的成交情况，因此，此次改进的项目特别关注度假产品的成交情况，以更精确地预测用户未来关于度假产品的成交行为。

### **2. 项目内容**
&emsp;&emsp;**该项目对下面四个地方进行了优化：**  
1. **特征提取优化：** 该项目将对特征提取进行全面的优化，围绕度假产品展开。这包括改进用户侧、流量侧、沟通侧和订单侧的特征提取方法，以增加特征间的区分度，以及更好地捕捉与度假产品相关的模式和规律。
2. **预测标签调整：** 将预测标签从原来的对所有产品成交情况修改为对度假产品成交情况的预测。这一调整旨在更精确地洞察度假产品的销售趋势，以便更好地满足用户需求。
3. **预测时间范围调整：** 为了解决正负样本不平衡的问题，该项目决定扩大预测标签的时间范围，从原来的预测未来一天成交情况，调整为预测未来七天内度假产品的成交情况。这一调整将提供更多的正样本数据，有助于改善模型的训练效果，减少不平衡性的影响。
4. **数据不平衡处理：** 该项目引入SMOTE算法来处理正负样本不平衡的问题。通过SMOTE算法，将对未成交样本进行降采样，同时对度假产品成交样本进行过采样，以实现数据集的平衡。这有助于改善模型对度假产品成交趋势的预测准确性。

&emsp;&emsp;通过以上改进，旨在提高模型的性能和准确性，以更好地预测用户在度假产品方面的成交趋势，希望为旅游平台提供更好的业务决策支持，帮助提高平台的业绩表现。

<br>



## <font color = 'blue' >二、导数</font>
### **1. 提取7月份有浏览或沟通行为的用户ID和最后一次行为发生时间** 
&emsp;&emsp;提取7月份有浏览或沟通行为的会员ID和最后一次浏览/沟通发生时间，并剔除已注销会员、内部会员、分销会员、黄牛会员的数据，结果保存在`tmp_cust_id_202307`。
``` sql
-- 查询2023.07有浏览或沟通记录的用户ID
drop table if exists tmp_cust_id_202307;
create table tmp_cust_id_202307 as
select cust_id, max(create_date) create_date
from (
    select distinct cast(vt_mapuid as bigint) cust_id, to_date(operate_time) create_date 
    from dw.kn1_traf_app_day_detail   -- APP流量
    where dt between '20230701' and '20230731' and to_date(operate_time) between '2023-07-01' and '2023-07-31' -- 过滤脏数据 
    union
    select distinct cast(vt_mapuid as bigint) cust_id, to_date(operate_time) create_date 
    from dw.kn1_traf_day_detail       -- PC/M站流量
    where dt between '20230701' and '20230731' and to_date(operate_time) between '2023-07-01' and '2023-07-31' -- 过滤脏数据 
    union
    select distinct b.user_id cust_id, create_date
    from (
        select 
            case when length(cust_tel_no) > 11 then substr(cust_tel_no, -11) else cust_tel_no end tel_num,
            to_date(status_start_time) create_date
        from dw.kn1_call_tel_order_detail           -- 电话呼入呼出
        where dt between '20230701' and '20230731' and status = '通话' and to_date(status_start_time) between '2023-07-01' and '2023-07-31'
    ) a
    left join (
        select user_id, feature_value as tel_num from dw.ol_rs_meta_feature_base_information
        where three_class_name = '手机号'
    ) b on b.tel_num = a.tel_num and a.tel_num is not null
    union
    select distinct cust_id, to_date(start_time) create_date
    from dw.kn1_sms_robot_outbound_call_detail      -- 智能外呼
    where dt between '20230701' and '20230731' and answer_flag = 1 and to_date(start_time) between '2023-07-01' and '2023-07-31'
    union
    select distinct cust_id, to_date(msg_time) create_date
    from dw.kn1_officeweixin_sender_cust_content    -- 企微聊天
    where dt between '20230701' and '20230731' and to_date(msg_time) between '2023-07-01' and '2023-07-31'
    union
    select distinct cust_id, to_date(create_start_time) create_date
    from dw.kn1_autotask_user_acsessed_chat         -- 在线客服
    where dt between '20230701' and '20230731' and to_date(create_start_time) between '2023-07-01' and '2023-07-31'
) t 
where cust_id in (
    select distinct cust_id from dw.kn1_usr_cust_attribute 
    where is_inside_staff = 0 and is_distribution = 0 and is_scalper = 0    -- 剔除内部会员/分销/黄牛
)   -- 未注销会员用户
    and cust_id between 1 and 1000000001
group by cust_id;
```

### **2. 提取用户未来n天关于度假产品的订单情况**
&emsp;&emsp;根据`tmp_cust_id_202307`中的会员ID和行为发生时间，查询该部分用户在行为发生后未来n天关于度假产品的下单、成交情况，结果保存在表`tmp_cust_order_202307`。
``` sql
-- 会员最后一次浏览/沟通后未来n天内度假产品的订单情况
drop table if exists tmp_cust_order_202307;
create table tmp_cust_order_202307 as
select 
    a.cust_id,                                          -- 会员ID
    a.create_date,                                      -- 会员最后一次浏览/沟通日期
    case when max(case when b.executed_date between date_add(a.create_date,1) and date_add(a.create_date,7) and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end executed_flag_7,              -- 会员最后一次浏览/沟通后未来7天内是否有度假产品的成交订单 0-否 1-是
    case when max(case when b.executed_date between date_add(a.create_date,1) and date_add(a.create_date,15) and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end executed_flag_15,             -- 会员最后一次浏览/沟通后未来15天内是否有度假产品的成交订单 0-否 1-是
    case when max(case when b.create_time between date_add(a.create_date,1) and date_add(a.create_date,7) and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end ordered_flag_7,               -- 会员最后一次浏览/沟通后未来7天内是否下单度假产品 0-否 1-是
    case when max(case when b.create_time between date_add(a.create_date,1) and date_add(a.create_date,15) and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end ordered_flag_15               -- 会员最后一次浏览/沟通后未来7天内是否下单度假产品 0-否 1-是
from tmp_cust_id_202307 a
left join (
    select 
        cust_id, route_id, book_city,
        to_date(create_time) create_time,
        if(cancel_flag=0 and sign_flag=1 and cancel_sign_flag=0, sign_date, null) executed_date
    from dw.kn2_ord_order_detail_all
    where dt = '20230815' and create_time >= '2023-07-01'   -- 统计7月份的订单
        and valid_flag=1 and is_sub = 0 and distribution_flag in (0, 3, 4, 5)
) b on b.cust_id = a.cust_id
left join (
    select 
        distinct route_id, book_city,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) c on c.route_id = b.route_id and c.book_city = b.book_city
group by a.cust_id, a.create_date;
```

### **3. 提取用户特征**
&emsp;&emsp;基于表`dw.kn1_usr_cust_attribute`的会员属性信息，联合用户画像基本信息，提取用户的个人基本信息和画像特征，结果保存在表`tmp_order_cust_feature`。
``` sql
-- 用户特征
drop table if exists tmp_cust_feature;
create table tmp_cust_feature as
select
    a.cust_id,                                                                  -- 会员ID
    a.create_date,                                                              -- 最后一次浏览/沟通日期
    b.cust_level,                                                               -- 会员星级
    b.cust_type,                                                                -- 会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他
    b.cust_group_type,                                                          -- 会员类型（1：普通散客；2：团队成员；3：团队会员）
    b.is_office_weixin,                                                         -- 是否企业微信用户:0否 1是
    if(a.create_date>b.rg_time, datediff(a.create_date, b.rg_time)+1, 0) rg_time, -- 注册时间/天
    if(a.create_date>b.office_weixin_time and b.office_weixin_time is not null, datediff(a.create_date, b.office_weixin_time)+1, 0) office_weixin_time,-- 添加企业微信时间/天
    rs1.sex,                                                                    -- 用户性别
    rs2.age,                                                                    -- 用户年龄
    rs3.age_group,                                                              -- 用户年龄段
    rs4.tel_num,                                                                -- 用户手机号
    rs5.user_city,                                                              -- 用户所在城市
    rs6.user_province,                                                          -- 用户所在省份
    nvl(rs7.access_channel_cnt, 0) access_channel_cnt,                          -- 可触达渠道的个数
    rs7.access_channels                                                         -- 可触达渠道：push/企业微信/短信
from tmp_cust_id_202307 a
left join (
    -- 会员属性
    select
        cust_id, cust_type, cust_level, to_date(rg_time) rg_time, cust_group_type, is_office_weixin, to_date(office_weixin_time) office_weixin_time
    from dw.kn1_usr_cust_attribute
    where cust_id in (select cust_id from tmp_cust_id_202307)
) b on b.cust_id = a.cust_id
left join (
    -- 会员基本信息-性别
    select user_id, feature_name as sex
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '性别'
        and user_id in (select cust_id from tmp_cust_id_202307)
) rs1 on a.cust_id = rs1.user_id
left join (
    -- 会员基本信息-年龄
    select user_id, feature_value as age
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '年龄'
        and user_id in (select cust_id from tmp_cust_id_202307)
) rs2 on a.cust_id = rs2.user_id
left join (
    -- 会员基本信息-年龄段
    select user_id, feature_name as age_group
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '年龄段'
        and user_id in (select cust_id from tmp_cust_id_202307)
) rs3 on a.cust_id = rs3.user_id
left join (
    -- 会员基本信息-手机号
    select user_id, feature_value as tel_num
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '手机号'
        and user_id in (select cust_id from tmp_cust_id_202307)
) rs4 on a.cust_id = rs4.user_id
left join (
    -- 基本属性--基本信息-用户所在城市
    select user_id, feature_name as user_city, feature_value as user_city_key
    from dw.ol_rs_meta_feature_basic_info
    where three_class_name = '所在城市'
        and user_id in (select cust_id from tmp_cust_id_202307)
) rs5 on a.cust_id = rs5.user_id
left join (
    -- 用户所在省份
    select city_key, parent_province_name as user_province
    from dw.kn1_pub_all_city_key
) rs6 on rs5.user_city_key = rs6.city_key
left join (
    -- 基本属性--个人信息-可触达渠道：push/企业微信/短信
    select 
        user_id, 
        count(1) as access_channel_cnt,
        concat_ws(',', collect_set(three_class_name)) as access_channels
    from dw.ol_rs_meta_feature_basic_info_access_channel
    where feature_value = 1 and user_id in (select cust_id from tmp_cust_id_202307)
    group by user_id
) rs7 on a.cust_id = rs7.user_id;
```

### **4. 基于7月份的订单信息，查询用户在最后一次行为发生时间之前的订单成交和投诉情况**
&emsp;&emsp;根据2023-01-01至2023-07-31的订单数据，计算出平均用户订单成交时间间隔为58.9天，故针对用户在最后一次行为发生前60天的订单情况进行统计。
&emsp;&emsp;统计了用户历史60天的订单成交量、订单投诉量、平均消费费用、最低消费费用、最高消费费用、平均回访满意度评分；度假产品的订单成交量、平均消费费用、平均回访满意度评分，结果保存在表`tmp_cust_historical_order`。
``` sql
-- 所有用户的平均订单成交时间间隔：58.9天
with ExecutedOrders as (
    select
        cust_id,
        order_id,
        executed_date
    from (
        SELECT 
            cust_id, order_id, 
            CASE
                WHEN cancel_flag = 0 AND sign_flag = 1 AND cancel_sign_flag = 0 THEN sign_date
                ELSE NULL
            END executed_date
        FROM dw.kn2_ord_order_detail_all
        WHERE dt = '20230731' AND create_time >= '2023-01-01'
            AND valid_flag = 1 AND is_sub = 0 AND distribution_flag IN (0, 3, 4, 5)
            AND cust_id IN (SELECT cust_id FROM tmp_cust_id_202307)
    ) ExecutedOrders
    WHERE executed_date IS NOT NULL
)
SELECT avg(avg_time_interval) all_avg_time_interval
FROM (
    SELECT
        eo.cust_id,
        AVG(DATEDIFF(eo.executed_date, prev.executed_date)) AS avg_time_interval
    FROM ExecutedOrders eo
    JOIN ExecutedOrders prev ON eo.cust_id = prev.cust_id AND eo.executed_date > prev.executed_date
    GROUP BY eo.cust_id
) t;
```
``` sql
-- 计算用户历史60天内成交订单数、投诉订单数
drop table if exists tmp_cust_historical_order;
create table tmp_cust_historical_order as
select
    a.cust_id,
    count(distinct case when b.executed_date between date_sub(a.create_date,59) and a.create_date then b.order_id else null end) executed_orders_cnt,                 -- 历史60天订单成交量
    count(distinct case when b.complaint_time between date_sub(a.create_date,59) and a.create_date then b.order_id else null end) complaint_orders_cnt,               -- 历史60天内订单投诉量
    nvl(avg(case when b.executed_date between date_sub(a.create_date,59) and a.create_date then b.travel_sign_amount else null end), 0) cp_avg,     -- 历史60天平均消费费用
    nvl(min(case when b.executed_date between date_sub(a.create_date,59) and a.create_date then b.travel_sign_amount else null end), 0) cp_min,     -- 历史60天最低消费费用
    nvl(max(case when b.executed_date between date_sub(a.create_date,59) and a.create_date then b.travel_sign_amount else null end), 0) cp_max,     -- 历史60天最高消费费用
    avg(case when b.return_visit_date between date_sub(a.create_date,59) and a.create_date then b.satisfaction else null end) satisfaction_avg,     -- 历史60天平均回访满意度评分
    count(distinct case when b.executed_date between date_sub(a.create_date,59) and a.create_date and c.producttype_class_name like '度假产品' then b.order_id else null end) vac_executed_cnt,   -- 历史60天内度假产品订单成交量
    nvl(avg(case when b.executed_date between date_sub(a.create_date,59) and a.create_date and c.producttype_class_name like '度假产品' then b.travel_sign_amount else null end), 0) vac_cp_avg,  -- 历史60天度假产品平均消费费用
    avg(case when b.return_visit_date between date_sub(a.create_date,59) and a.create_date and c.producttype_class_name like '度假产品' then b.satisfaction else null end) vac_satisfaction_avg   -- 历史60天度假产品平均回访满意度评分
from tmp_cust_id_202307 a
left join (
    select 
        cust_id, order_id, 
        if(cancel_flag=0 and sign_flag=1 and cancel_sign_flag=0, sign_date, null) executed_date,    -- 订单是否成交 0-否 1-是
        to_date(complaint_time) complaint_time, -- 投诉时间
        travel_sign_amount,     -- 订单价格
        satisfaction,           -- 回访满意度总分
        return_visit_date,      -- 回访日期
        route_id, book_city
    from dw.kn2_ord_order_detail_all
    where dt = '20230731' and create_time >= date_sub('2023-07-01', 59)
        and valid_flag=1 and is_sub = 0 and distribution_flag in (0, 3, 4, 5)
) b on b.cust_id = a.cust_id
left join (
    select 
        distinct route_id, book_city,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) c on c.route_id = b.route_id and c.book_city = b.book_city
group by a.cust_id;
```

### **5. 提取用户的浏览行为特征和沟通行为特征**
&emsp;&emsp;首先，统计该部分用户历史15天内在APP/PC/M站的浏览记录、浏览产品的相关信息、领取优惠券情况，结果保存在表`tmp_cust_browsing_info`。
&emsp;&emsp;其次，统计该部分用户在电话呼入呼出、智能外呼、企微聊天、在线客服这四个渠道的沟通记录、沟通时间、沟通内容等相关数据，结果保存在表`tmp_cust_communication_info`。
&emsp;&emsp;最后，基于这两张表，计算用户的决策时间、浏览行为特征和沟通行为特征，结果保存在表`tmp_cust_behavior_info`。
``` sql
--  用户的浏览行为信息
drop table if exists tmp_cust_browsing_info;
create table tmp_cust_browsing_info as
select
    a.cust_id,                                              -- 会员ID
    a.create_date,                                          -- 用户最后一次浏览/沟通日期
    b.operate_date,                                         -- 用户访问日期
    -- if(b.operate_date between date_sub(a.create_date, 14) and a.create_date, 1, 0) browsing_flag, -- 最后一次浏览/沟通前15天是否进行浏览 0-否 1-是
    case when b.operate_date between date_sub(a.create_date, 14) and a.create_date then 1
        when b.operate_date < date_sub(a.create_date, 14) and b.operate_date is not null then 2
        else 0
    end browsing_flag,
    b.visitor_trace,                                        -- 访客标记
    b.residence_time,                                       -- 页面停留时间
    b.product_id,                                           -- 产品ID
    b.book_id,                                              -- 预订页ID
    b.search_key,                                           -- 搜索词
    b.search_key_type,                                      -- 搜索词类型 1-度假产品 0-单资源产品 null-无搜索词
    c.lowest_price,                                         -- 产品最低价
    d.producttype_class_name,                               -- 浏览产品大类
    nvl(e.collect_coupons_status, 0) collect_coupons_status,-- 优惠券领取状态 1-成功 0-未领取/失败
    f.f_satisfaction,                                       -- 产品平均满意度
    f.f_comp_grade_level,                                   -- 总评级别
    f.f_remark_amount,                                      -- 点评数目
    f.f_essence_amount,                                     -- 精华点评数目
    f.f_coupon_amount,                                      -- 返抵用券金额
    f.f_money_amount,                                       -- 返现金额
    f.f_photo_amount                                        -- 图片数目
from tmp_cust_id_202307 a 
left join (
    -- 流量域
    select 
        distinct vt_mapuid, to_date(operate_time) operate_date, visitor_trace, residence_time, product_id, book_id, search_key, 
        case when search_key like '%票%' or search_key like '%酒店%' then 0 
            when search_key is null or lower(search_key) = 'null' then null
            else 1
        end search_key_type
    from dw.kn1_traf_app_day_detail
    where dt between '20230615' and '20230731'
        and vt_mapuid in (select cust_id from tmp_cust_id_202307)
        and to_date(operate_time) between '2023-06-15' and '2023-07-31'
    union
    select 
        distinct vt_mapuid, to_date(operate_time) operate_date, visitor_trace, residence_time, product_id, book_id, search_key, 
        case when search_key like '%票%' or search_key like '%酒店%' then 0 
            when lower(search_key) is null or lower(search_key) = 'null' then null
            else 1
        end search_key_type
    from dw.kn1_traf_day_detail
    where dt between '20230615' and '20230731'
        and vt_mapuid in (select cust_id from tmp_cust_id_202307)
        and to_date(operate_time) between '2023-06-15' and '2023-07-31'
) b on b.vt_mapuid = a.cust_id
left join (
    -- 产品最低价
    select distinct route_id, lowest_price
    from dw.kn1_prd_route
) c on c.route_id = b.product_id
left join (
    select 
        distinct route_id,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) d on d.route_id = b.product_id
left join (
    -- 用户领券情况
    select distinct cust_id, collect_coupons_status, to_date(operate_time) operate_date
    from dw.ods_crmmkt_mkt_scene_clear_intention_cust_transform
    where to_date(operate_time) between '2023-06-15' and '2023-07-31'
        and collect_coupons_status = 1 and cust_id in (select cust_id from tmp_cust_id_202307)
) e on e.cust_id = b.vt_mapuid and e.operate_date = b.operate_date
left join (
    -- 获取产品关联的满意度和评价总人数
    select 
        t1.f_product_id, f_satisfaction / 1000000 f_satisfaction, f_comp_grade_level, f_remark_amount, 
        f_essence_amount, f_coupon_amount, f_money_amount, f_photo_amount
    from dw.ods_ncs_t_nremark_stat_product t1
    join (
        select f_product_id, max(f_update_time) max_update_time
        from dw.ods_ncs_t_nremark_stat_product
        group by f_product_id
    ) t2 ON t1.f_product_id = t2.f_product_id and t1.f_update_time = t2.max_update_time
) f on f.f_product_id = b.product_id;
```
``` sql
-- 用户的沟通行为信息
drop table if exists tmp_cust_communication_info;
create table tmp_cust_communication_info as
select
    a.cust_id,                              -- 会员编号
    a.create_date,                          -- 用户最后一次浏览/沟通日期
    b.comm_date,                            -- 沟通日期
    -- if(b.comm_date between date_sub(a.create_date, 14) and a.create_date, 1, 0) comm_flag, -- 最后一次浏览/沟通前15天是否进行沟通 0-否 1-是
    case when b.comm_date between date_sub(a.create_date, 14) and a.create_date then 1
        when b.comm_date < date_sub(a.create_date, 14) and b.comm_date is not null then 2
        else 0
    end comm_flag,
    b.channel,                              -- 沟通渠道名称
    b.channel_id,                           -- 沟通渠道ID 1-电话呼入呼出 2-智能外呼 3-企微聊天 4-在线客服
    b.comm_duration,                        -- 沟通持续时长
    b.day_calls,                            -- 每日电话呼入呼出/智能外呼次数
    b.comm_num,                             -- 沟通量：通话量/聊天数
    b.active_comm_num,                      -- 用户主动进行沟通的数量
    b.vac_mention_num,                      -- 沟通过程中度假产品的提及次数
    b.single_mention_num                    -- 沟通过程中单资源产品的提及次数
from tmp_cust_id_202307 a 
left join (
    -- 电话明细
    select
        t2.cust_id, t1.comm_date,
        sum(t1.status_time) comm_duration,  -- 通话总时长
        count(1) day_calls,                 -- 每日电话呼入/呼出次数
        sum(case when t1.status='通话' then 1 else 0 end) comm_num,        -- 接通量
        sum(case when t1.status='通话' and t1.calldir='呼入' then 1 else 0 end) active_comm_num,-- 用户呼入量
        sum(case when t4.producttype_class_name like '度假产品' then 1 else 0 end) vac_mention_num,
        sum(case when t4.producttype_class_name like '单资源产品' then 1 else 0 end) single_mention_num,
        '电话呼入呼出' as channel, 1 as channel_id
    from (
        select
            case when length(cust_tel_no) > 11 then substr(cust_tel_no, -11) else cust_tel_no end tel_num,
            to_date(status_start_time) comm_date,
            status_time, status, calldir, order_id
        from dw.kn1_call_tel_order_detail
        where dt between '20230615' and '20230731' and to_date(status_start_time) between '2023-06-15' and '2023-07-31' and tel_order_type in (0, 1)     -- 通话时间在下单时间之前
    ) t1 
    left join (select cust_id, tel_num from tmp_cust_feature) t2
    on t2.tel_num = t1.tel_num
    left join (
        select order_id, route_id, book_city from dw.kn2_ord_order_detail_all
        where dt = '20230731' and create_time >= '2023-06-01'
    ) t3 on t3.order_id = t1.order_id
    left join (
        select distinct route_id, book_city,
            case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
        from dw.kn2_dim_route_product_sale
    ) t4 on t4.route_id = t3.route_id and t4.book_city = t3.book_city
    group by cust_id, comm_date
    union all
    -- 机器人外呼明细
    select
        cust_id, to_date(start_time) comm_date,
        sum(call_time) comm_duration,           -- 通话总时长
        count(1) day_calls,                     -- 每日外呼次数
        sum(answer_flag) comm_num,              -- 接通量
        sum(case when answer_flag=1 and generate_task_is = 1 and lower(label) in ('a','b','q','c','p','s') then 1 else 0 end) active_comm_num,   -- 命中用户出游意向量
        0 as vac_mention_num,
        0 as single_mention_num,
        '智能外呼' as channel, 2 as channel_id
    from dw.kn1_sms_robot_outbound_call_detail
    where dt between '20230615' and '20230731' and to_date(start_time) between '2023-06-15' and '2023-07-31'
        and cust_id in (select cust_id from tmp_cust_id_202307)
    group by cust_id, to_date(start_time)
    union all
    -- 企微聊天明细
    select 
        cust_id, to_date(msg_time) comm_date,
        null as comm_duration,                  -- 聊天时长：null
        0 as day_calls,
        count(msg_time) comm_num,               -- 发送消息数
        sum(case when type=1 then 1 else 0 end) active_comm_num,   -- 用户主动聊天数
        sum(case when contact like '%商旅%' or contact like '%跟团%' or contact like '%自驾%' or contact like '%自助%'
                or contact like '%目的地服务%' or contact like '%签证%' or contact like '%团队%' or contact like '%定制%'
                or contact like '%游轮%' or contact like '%旅拍%' or contact like '%游%' or contact like '%团%' then 1 else 0 end) vac_mention_num,
        sum(case when contact like '%火车票%' or contact like '%机票%' or contact like '%酒店%' or contact like '%百货%'
                or contact like '%用车服务%' or contact like '%高铁%' or contact like '%票%' or contact like '%硬座%'
                or contact like '%软卧%' or contact like '%卧铺%' or contact like '%航班%' then 1 else 0 end) single_mention_num,
        '企微聊天' as channel, 3 as channel_id
    from dw.kn1_officeweixin_sender_cust_content
    where dt between '20230615' and '20230731' and to_date(msg_time) between '2023-06-15' and '2023-07-31'
        and cust_id in (select cust_id from tmp_cust_id_202307)
    group by cust_id, to_date(msg_time)
    union all
    -- 在线客服沟通明细
    select
        cust_id,
        to_date(create_start_time) comm_date,
        null as comm_duration,                  -- 聊天时长：null
        0 as day_calls,
        count(1) comm_num,                      -- 发送消息数
        sum(case when content like '%客人发送消息%' then 1 else 0 end) active_comm_num,    -- 用户主动发送消息数
        sum(case when content like '%商旅%' or content like '%跟团%' or content like '%自驾%' or content like '%自助%'
                or content like '%目的地服务%' or content like '%签证%' or content like '%团队%' or content like '%定制%'
                or content like '%游轮%' or content like '%旅拍%' or content like '%游%' or content like '%团%' then 1 else 0 end) vac_mention_num,
        sum(case when content like '%火车票%' or content like '%机票%' or content like '%酒店%' or content like '%百货%'
                or content like '%用车服务%' or content like '%高铁%' or content like '%票%' or content like '%硬座%'
                or content like '%软卧%' or content like '%卧铺%' or content like '%航班%' then 1 else 0 end) single_mention_num,
        '在线客服' as channel, 4 as channel_id
    from dw.kn1_autotask_user_acsessed_chat
    where dt between '20230615' and '20230731' and to_date(create_start_time) between '2023-06-15' and '2023-07-31'
        and cust_id in (select cust_id from tmp_cust_id_202307)
    group by cust_id, to_date(create_start_time)
) b on b.cust_id = a.cust_id;
```
``` sql
-- 计算用户的行为特征——浏览特征 + 沟通特征
drop table if exists tmp_cust_behavior_info;
create table tmp_cust_behavior_info as
select
    a.cust_id,                                                                                      -- 会员ID
    b.decision_time,                                                                                -- 决策时间
    nvl(c.browsing_days, 0) browsing_days,                                                          -- 历史浏览天数 
    nvl(c.pv, 0) pv,                                                                                -- 用户总pv
    nvl(c.pv_daily_avg, 0) pv_daily_avg,                                                            -- 每日平均pv
    nvl(c.browsing_time, 0) browsing_time,                                                          -- 用户总浏览时间
    nvl(c.browsing_time_max, 0) browsing_time_max,                                                  -- 单次浏览的最大浏览时间
    nvl(c.browsing_time_daily_avg, 0) browsing_time_daily_avg,                                      -- 每日平均浏览时间
    nvl(c.browsing_products_cnt, 0) browsing_products_cnt,                                          -- 历史浏览产品量
    nvl(c.browsing_products_cnt_daily_avg, 0) browsing_products_cnt_daily_avg,                      -- 每日平均浏览产品量
    nvl(c.to_booking_pages_cnt, 0) to_booking_pages_cnt,                                            -- 到预定页数
    nvl(c.search_times, 0) search_times,                                                            -- 搜索次数
    nvl(c.vac_search_times, 0) vac_search_times,                                                    -- 度假产品搜索次数
    c.lowest_price_avg,                                                                             -- 历史浏览产品（最低价）平均价
    c.vac_lowest_price_avg,                                                                         -- 历史浏览度假产品的平均最低价
    nvl(c.browsing_vac_prd_cnt, 0) browsing_vac_prd_cnt,                                            -- 历史浏览的度假产品数量
    nvl(c.browsing_single_prd_cnt, 0) browsing_single_prd_cnt,                                      -- 历史浏览的单资源产品数量
    nvl(c.collect_coupons_cnt, 0) collect_coupons_cnt,                                              -- 优惠券领券个数
    c.prd_satisfaction,                                                                             -- 平均产品满意度
    c.prd_comp_grade_level,                                                                         -- 最大产品总评级别
    nvl(c.prd_remark_amount, 0) prd_remark_amount,                                                  -- 平均点评数目
    nvl(c.prd_essence_amount, 0) prd_essence_amount,                                                -- 平均精华点评数目
    nvl(c.prd_coupon_amount, 0) prd_coupon_amount,                                                  -- 平均返抵用券金额
    nvl(c.prd_money_amount, 0) prd_money_amount,                                                    -- 平均返现金额
    nvl(c.prd_photo_amount, 0) prd_photo_amount,                                                    -- 平均音频数目
    nvl(d.comm_days, 0) comm_days,                                                                  -- 历史沟通天数
    nvl(d.comm_freq, 0) comm_freq,                                                                  -- 总沟通次数（count(channel)）
    nvl(d.comm_freq_daily_avg, 0) comm_freq_daily_avg,                                              -- 每日平均沟通次数
    nvl(d.call_pct, 0) call_pct,                                                                    -- 电话呼入呼出占比（电话呼入呼出/总沟通次数）
    nvl(d.robot_pct, 0) robot_pct,                                                                  -- 智能外呼占比
    nvl(d.officewx_pct, 0) officewx_pct,                                                            -- 企微聊天占比
    nvl(d.chat_pct, 0) chat_pct,                                                                    -- 在线客服占比
    nvl(d.channels_cnt, 0) as channels_cnt,                                                         -- 历史沟通渠道数（count(distinct channel)）
    nvl(d.comm_time, 0) comm_time,                                                                  -- 总沟通时长（特指电话、智能外呼）
    nvl(d.comm_time_daily_avg, 0) comm_time_daily_avg,                                              -- 每日平均沟通时长
    nvl(d.call_completing_rate, 0) call_completing_rate,                                            -- 电话接通率（特指电话、智能外呼）
    nvl(d.calls_freq, 0) calls_freq,                                                                -- 通话频数：电话+智能外呼
    nvl(d.calls_freq_daily_avg, 0) calls_freq_daily_avg,                                            -- 每日平均通话频数：电话+智能外呼
    nvl(d.active_calls_freq, 0) active_calls_freq,                                                  -- 用户主动通话频数：电话+智能外呼
    nvl(d.active_calls_pct, 0) active_calls_pct,                                                    -- 用户主动通话占比 = 用户主动通话频数 / 通话频数
    nvl(d.chats_freq, 0) chats_freq,                                                                -- 聊天频数：企微聊天+在线客服
    nvl(d.chats_freq_daily_avg, 0) chats_freq_daily_avg,                                            -- 每日平均聊天频数：企微聊天+在线客服
    nvl(d.active_chats_freq, 0) active_chats_freq,                                                  -- 用户主动聊天频数：企微聊天+在线客服
    nvl(d.active_chats_pct, 0) active_chats_pct,                                                    -- 用户主动聊天占比 = 用户主动聊天频数 / 聊天频数
    nvl(d.vac_mention_num, 0) vac_mention_num,                                                      -- 沟通过程中度假产品的总提及次数
    nvl(d.single_mention_num, 0) single_mention_num                                                 -- 沟通过程中度假产品的总提及次数
from tmp_cust_id_202307 a
left join (
    -- 计算决策时间
    select 
        t1.cust_id,
        case when max(browsing_flag)=2 or max(comm_flag)=2 then 16
            when max(browsing_flag)=1 or max(comm_flag)=1 then max(datediff(create_date, least(nvl(operate_date,create_date), nvl(comm_date,create_date)))) + 1
            else 0
        end decision_time
    from (select cust_id, create_date, operate_date, browsing_flag from tmp_cust_browsing_info) t1
    left join (select cust_id, comm_date, comm_flag from tmp_cust_communication_info) t2
    on t1.cust_id = t2.cust_id
    group by t1.cust_id
) b on b.cust_id = a.cust_id
left join (
    select  
        cust_id,
        min(operate_date) first_operate_date,
        count(distinct operate_date) browsing_days,
        count(visitor_trace) pv,
        round(count(visitor_trace) / count(distinct operate_date), 4) pv_daily_avg,
        sum(residence_time) browsing_time,
        max(residence_time) browsing_time_max,
        round(sum(residence_time) / count(distinct operate_date), 4) browsing_time_daily_avg,
        count(distinct product_id) browsing_products_cnt,
        round(count(distinct product_id) / count(distinct operate_date), 4) browsing_products_cnt_daily_avg,
        count(book_id) to_booking_pages_cnt,
        count(search_key) search_times,
        sum(search_key_type) vac_search_times,
        round(avg(lowest_price), 4) lowest_price_avg,
        round(avg(case when producttype_class_name like '度假产品' then lowest_price else null end), 4) vac_lowest_price_avg,
        count(distinct case when producttype_class_name like '度假产品' then product_id else null end) browsing_vac_prd_cnt,
        count(distinct case when producttype_class_name like '单资源产品' then product_id else null end) browsing_single_prd_cnt,
        count(distinct case when collect_coupons_status = 1 then operate_date else null end) collect_coupons_cnt,
        round(avg(f_satisfaction), 4) prd_satisfaction,
        max(f_comp_grade_level) prd_comp_grade_level,
        round(avg(f_remark_amount), 4) prd_remark_amount,
        round(avg(f_essence_amount), 4) prd_essence_amount,
        round(avg(f_coupon_amount), 4) prd_coupon_amount,
        round(avg(f_money_amount), 4) prd_money_amount,
        round(avg(f_photo_amount), 4) prd_photo_amount
    from tmp_cust_browsing_info
    where browsing_flag = 1         -- 只对有浏览行为的用户计算指标
    group by cust_id
) c on c.cust_id = a.cust_id
left join (
    select  
        cust_id,
        min(comm_date) first_comm_date,
        count(distinct comm_date) comm_days,
        count(channel_id) comm_freq,
        round(count(channel_id)/count(distinct comm_date), 4) comm_freq_daily_avg,
        round(sum(if(channel_id=1,1,0)) / count(channel_id) * 100, 4) call_pct,
        round(sum(if(channel_id=2,1,0)) / count(channel_id) * 100, 4) robot_pct,
        round(sum(if(channel_id=3,1,0)) / count(channel_id) * 100, 4) officewx_pct,
        round(sum(if(channel_id=4,1,0)) / count(channel_id) * 100, 4) chat_pct,
        count(distinct channel_id) channels_cnt,
        sum(comm_duration) comm_time,
        round(sum(comm_duration) / count(distinct comm_date), 4) comm_time_daily_avg,
        if(sum(day_calls)<>0, round(sum(if(channel_id in (1,2), comm_num, 0)) / sum(day_calls) * 100, 4), 0) call_completing_rate,
        sum(if(channel_id in (1,2), comm_num, 0)) calls_freq,
        round(sum(if(channel_id in (1,2), comm_num, 0)) / count(distinct if(channel_id in (1,2), comm_date, null)), 4) calls_freq_daily_avg,
        sum(if(channel_id in (1,2), active_comm_num, 0)) active_calls_freq,
        round(sum(if(channel_id in (1,2), active_comm_num, 0)) / sum(if(channel_id in (1,2), comm_num, 0)) * 100, 4) active_calls_pct,
        sum(if(channel_id in (3,4), comm_num, 0)) chats_freq,
        round(sum(if(channel_id in (3,4), comm_num, 0)) / count(distinct if(channel_id in (3,4), comm_date, null)), 4) chats_freq_daily_avg,
        sum(if(channel_id in (3,4), active_comm_num, 0)) active_chats_freq,
        round(sum(if(channel_id in (3,4), active_comm_num, 0)) / sum(if(channel_id in (3,4), comm_num, 0)) * 100, 4) active_chats_pct,
        sum(vac_mention_num) vac_mention_num,
        sum(single_mention_num) single_mention_num
    from tmp_cust_communication_info
    where comm_flag = 1         -- 只对有沟通行为的用户计算指标
    group by cust_id
) d on d.cust_id = a.cust_id;
```

### **6. 数据导出及数据表基本信息汇总**
&emsp;&emsp;汇总以上临时表，存储用户特征信息、用户在历史15天内的订单成交和投诉信息、沟通和浏览行为信息，以及用户在未来n天关于度假产品的下单、成交情况，结果保存在表`tmp_cust_order_pred_202307`。
``` sql
-- 汇总以上表
drop table if exists tmp_cust_order_pred_202307;
create table tmp_cust_order_pred_202307 as
select
    t1.cust_id,                                                 -- 会员ID
    t1.create_date,                                             -- 会员最后一次浏览/沟通日期
    t1.executed_flag_7,                                         -- 会员最后一次浏览/沟通后未来7天内是否有度假产品的成交订单 0-否 1-是
    t1.executed_flag_15,                                        -- 会员最后一次浏览/沟通后未来15天内是否有度假产品的成交订单 0-否 1-是
    t1.ordered_flag_7,                                          -- 会员最后一次浏览/沟通后未来7天内是否下单度假产品 0-否 1-是
    t1.ordered_flag_15,                                         -- 会员最后一次浏览/沟通后未来15天内是否下单度假产品 0-否 1-是
    t2.cust_level,                                              -- 会员星级
    t2.cust_type,                                               -- 会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他
    t2.cust_group_type,                                         -- 会员类型（1：普通散客；2：团队成员；3：团队会员）
    t2.is_office_weixin,                                        -- 是否企业微信用户:0否 1是
    t2.rg_time,                                                 -- 注册时间/天
    t2.office_weixin_time,                                      -- 添加企业微信时间/天
    t2.sex,                                                     -- 用户性别
    t2.age,                                                     -- 用户年龄
    t2.age_group,                                               -- 用户年龄段
    t2.access_channel_cnt,                                      -- 可触达渠道的个数
    t3.executed_orders_cnt,                                     -- 历史60天订单成交量
    t3.complaint_orders_cnt,                                    -- 历史60天内订单投诉量
    t3.cp_avg,                                                  -- 历史60天平均消费费用
    t3.cp_min,                                                  -- 历史60天最低消费费用
    t3.cp_max,                                                  -- 历史60天最高消费费用
    t3.satisfaction_avg,                                        -- 历史60天平均回访满意度评分
    t3.vac_executed_cnt,                                        -- 历史60天内度假产品订单成交量
    t3.vac_cp_avg,                                              -- 历史60天度假产品平均消费费用
    t3.vac_satisfaction_avg,                                    -- 历史60天度假产品平均回访满意度评分
    t4.decision_time,                                           -- 决策时间
    t4.browsing_days,                                           -- 历史浏览天数 
    t4.pv,                                                      -- 用户总pv
    t4.pv_daily_avg,                                            -- 每日平均pv
    t4.browsing_time,                                           -- 用户总浏览时间
    t4.browsing_time_max,                                       -- 单次浏览的最大浏览时间
    t4.browsing_time_daily_avg,                                 -- 每日平均浏览时间
    t4.browsing_products_cnt,                                   -- 历史浏览产品量
    t4.browsing_products_cnt_daily_avg,                         -- 每日平均浏览产品量
    t4.to_booking_pages_cnt,                                    -- 到预定页数
    t4.search_times,                                            -- 搜索次数
    t4.vac_search_times,                                        -- 度假产品搜索次数
    t4.lowest_price_avg,                                        -- 历史浏览产品（最低价）平均价
    t4.vac_lowest_price_avg,                                    -- 历史浏览度假产品的平均最低价
    t4.browsing_vac_prd_cnt,                                    -- 历史浏览的度假产品数量
    t4.browsing_single_prd_cnt,                                 -- 历史浏览的单资源产品数量
    t4.collect_coupons_cnt,                                     -- 优惠券领券个数
    t4.prd_satisfaction,                                        -- 平均产品满意度
    t4.prd_comp_grade_level,                                    -- 最大产品总评级别
    t4.prd_remark_amount,                                       -- 平均点评数目
    t4.prd_essence_amount,                                      -- 平均精华点评数目
    t4.prd_coupon_amount,                                       -- 平均返抵用券金额
    t4.prd_money_amount,                                        -- 平均返现金额
    t4.prd_photo_amount,                                        -- 平均音频数目
    t4.comm_days,                                               -- 历史沟通天数
    t4.comm_freq,                                               -- 总沟通次数（count(channel)）
    t4.comm_freq_daily_avg,                                     -- 每日平均沟通次数
    t4.call_pct,                                                -- 电话呼入呼出占比（电话呼入呼出/总沟通次数）
    t4.robot_pct,                                               -- 智能外呼占比
    t4.officewx_pct,                                            -- 企微聊天占比
    t4.chat_pct,                                                -- 在线客服占比
    t4.channels_cnt,                                            -- 历史沟通渠道数（count(distinct channel)）
    t4.comm_time,                                               -- 总沟通时长（特指电话、智能外呼）
    t4.comm_time_daily_avg,                                     -- 每日平均沟通时长
    t4.call_completing_rate,                                    -- 电话接通率（特指电话、智能外呼）
    t4.calls_freq,                                              -- 通话频数：电话+智能外呼
    t4.calls_freq_daily_avg,                                    -- 每日平均通话频数：电话+智能外呼
    t4.active_calls_freq,                                       -- 用户主动通话频数：电话+智能外呼
    t4.active_calls_pct,                                        -- 用户主动通话占比 = 用户主动通话频数 / 通话频数
    t4.chats_freq,                                              -- 聊天频数：企微聊天+在线客服
    t4.chats_freq_daily_avg,                                    -- 每日平均聊天频数：企微聊天+在线客服
    t4.active_chats_freq,                                       -- 用户主动聊天频数：企微聊天+在线客服
    t4.active_chats_pct,                                        -- 用户主动聊天占比 = 用户主动聊天频数 / 聊天频数
    t4.vac_mention_num,                                         -- 沟通过程中度假产品的总提及次数
    t4.single_mention_num                                       -- 沟通过程中度假产品的总提及次数
from tmp_cust_order_202307 t1
left join tmp_cust_feature t2
on t2.cust_id = t1.cust_id
left join tmp_cust_historical_order t3
on t3.cust_id = t1.cust_id
left join tmp_cust_behavior_info t4
on t4.cust_id = t1.cust_id;
```

&emsp;&emsp;特征说明如下：
<!-- 
<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-peve{background-color:#FFFAE5;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-nfft{background-color:#FFFAE5;text-align:left;vertical-align:top}
.tg .tg-pc0k{background-color:#FFF8B9;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-0lax{text-align:left;vertical-align:top}
</style> 
-->
<table class="tg">
<thead>
  <tr>
    <th class="tg-pc0k"><span style="font-weight:bold">特征维度</span></th>
    <th class="tg-pc0k"><span style="font-weight:bold;color:#000">Feature</span></th>
    <th class="tg-pc0k"><span style="font-weight:bold;color:#000">Meaning</span></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-nfft"></td>
    <td class="tg-0lax"><span style="color:#000">cust_id</span></td>
    <td class="tg-0lax"><span style="color:#000">会员ID</span></td>
  </tr>
  <tr>
    <td class="tg-nfft"></td>
    <td class="tg-0lax"><span style="color:#000">create_date</span></td>
    <td class="tg-0lax"><span style="color:#000">会员最后一次浏览/沟通日期</span></td>
  </tr>
  <tr>
    <td class="tg-peve" rowspan="4"><span style="font-weight:bold;color:#000">预测标签</span></td>
    <td class="tg-0lax"><span style="color:#000">executed_flag_7</span></td>
    <td class="tg-0lax"><span style="color:#000">会员最后一次浏览/沟通后未来7天内是否有度假产品的成交订单 0-否 1-是</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">executed_flag_15</span></td>
    <td class="tg-0lax"><span style="color:#000">会员最后一次浏览/沟通后未来15天内是否有度假产品的成交订单 0-否 1-是</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">ordered_flag_7</span></td>
    <td class="tg-0lax"><span style="color:#000">会员最后一次浏览/沟通后未来7天内是否下单度假产品 0-否 1-是</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">ordered_flag_15</span></td>
    <td class="tg-0lax"><span style="color:#000">会员最后一次浏览/沟通后未来15天内是否下单度假产品 0-否 1-是</span></td>
  </tr>
  <tr>
    <td class="tg-peve" rowspan="10"><span style="font-weight:bold;color:#000">用户特征</span></td>
    <td class="tg-0lax"><span style="color:#000">cust_level</span></td>
    <td class="tg-0lax"><span style="color:#000">会员星级</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">cust_type</span></td>
    <td class="tg-0lax"><span style="color:#000">会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">cust_group_type</span></td>
    <td class="tg-0lax"><span style="color:#000">会员类型（1：普通散客；2：团队成员；3：团队会员）</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">is_office_weixin</span></td>
    <td class="tg-0lax"><span style="color:#000">是否企业微信用户:0否 1是</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">rg_time</span></td>
    <td class="tg-0lax"><span style="color:#000">注册时间/天</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">office_weixin_time</span></td>
    <td class="tg-0lax"><span style="color:#000">添加企业微信时间/天</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">sex</span></td>
    <td class="tg-0lax"><span style="color:#000">用户性别</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">age</span></td>
    <td class="tg-0lax"><span style="color:#000">用户年龄</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">age_group</span></td>
    <td class="tg-0lax"><span style="color:#000">用户年龄段</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">access_channel_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">可触达渠道的个数</span></td>
  </tr>
  <tr>
    <td class="tg-peve" rowspan="9"><span style="font-weight:bold;color:#000">历史成交订单特征</span></td>
    <td class="tg-0lax"><span style="color:#000">executed_orders_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天订单成交量</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">complaint_orders_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天内订单投诉量</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">cp_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天平均消费费用</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">cp_min</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天最低消费费用</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">cp_max</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天最高消费费用</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">satisfaction_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天平均回访满意度评分</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">vac_executed_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天内度假产品订单成交量</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">vac_cp_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天度假产品平均消费费用</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">vac_satisfaction_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">历史60天度假产品平均回访满意度评分</span></td>
  </tr>
  <tr>
    <td class="tg-nfft"></td>
    <td class="tg-0lax"><span style="color:#000">decision_time</span></td>
    <td class="tg-0lax"><span style="color:#000">决策时间</span></td>
  </tr>
  <tr>
    <td class="tg-peve" rowspan="23"><span style="font-weight:bold;color:#000">浏览行为特征</span></td>
    <td class="tg-0lax"><span style="color:#000">browsing_days</span></td>
    <td class="tg-0lax"><span style="color:#000">历史浏览天数 </span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">pv</span></td>
    <td class="tg-0lax"><span style="color:#000">用户总pv</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">pv_daily_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">每日平均pv</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">browsing_time</span></td>
    <td class="tg-0lax"><span style="color:#000">用户总浏览时间</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">browsing_time_max</span></td>
    <td class="tg-0lax"><span style="color:#000">单次浏览的最大浏览时间</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">browsing_time_daily_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">每日平均浏览时间</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">browsing_products_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">历史浏览产品量</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">browsing_products_cnt_daily_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">每日平均浏览产品量</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">to_booking_pages_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">到预定页数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">search_times</span></td>
    <td class="tg-0lax"><span style="color:#000">搜索次数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">vac_search_times</span></td>
    <td class="tg-0lax"><span style="color:#000">度假产品搜索次数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">lowest_price_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">历史浏览产品（最低价）平均价</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">vac_lowest_price_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">历史浏览度假产品的平均最低价</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">browsing_vac_prd_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">历史浏览的度假产品数量</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">browsing_single_prd_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">历史浏览的单资源产品数量</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">collect_coupons_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">优惠券领券个数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">prd_satisfaction</span></td>
    <td class="tg-0lax"><span style="color:#000">平均产品满意度</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">prd_comp_grade_level</span></td>
    <td class="tg-0lax"><span style="color:#000">最大产品总评级别</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">prd_remark_amount</span></td>
    <td class="tg-0lax"><span style="color:#000">平均点评数目</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">prd_essence_amount</span></td>
    <td class="tg-0lax"><span style="color:#000">平均精华点评数目</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">prd_coupon_amount</span></td>
    <td class="tg-0lax"><span style="color:#000">平均返抵用券金额</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">prd_money_amount</span></td>
    <td class="tg-0lax"><span style="color:#000">平均返现金额</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">prd_photo_amount</span></td>
    <td class="tg-0lax"><span style="color:#000">平均音频数目</span></td>
  </tr>
  <tr>
    <td class="tg-peve" rowspan="21"><span style="font-weight:bold;color:#000">沟通行为特征</span></td>
    <td class="tg-0lax"><span style="color:#000">comm_days</span></td>
    <td class="tg-0lax"><span style="color:#000">历史沟通天数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">comm_freq</span></td>
    <td class="tg-0lax"><span style="color:#000">总沟通次数（count(channel)）</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">comm_freq_daily_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">每日平均沟通次数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">call_pct</span></td>
    <td class="tg-0lax"><span style="color:#000">电话呼入呼出占比（电话呼入呼出/总沟通次数）</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">robot_pct</span></td>
    <td class="tg-0lax"><span style="color:#000">智能外呼占比</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">officewx_pct</span></td>
    <td class="tg-0lax"><span style="color:#000">企微聊天占比</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">chat_pct</span></td>
    <td class="tg-0lax"><span style="color:#000">在线客服占比</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">channels_cnt</span></td>
    <td class="tg-0lax"><span style="color:#000">历史沟通渠道数（count(distinct channel)）</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">comm_time</span></td>
    <td class="tg-0lax"><span style="color:#000">总沟通时长（特指电话、智能外呼）</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">comm_time_daily_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">每日平均沟通时长</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">call_completing_rate</span></td>
    <td class="tg-0lax"><span style="color:#000">电话接通率（特指电话、智能外呼）</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">calls_freq</span></td>
    <td class="tg-0lax"><span style="color:#000">通话频数：电话+智能外呼</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">calls_freq_daily_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">每日平均通话频数：电话+智能外呼</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">active_calls_freq</span></td>
    <td class="tg-0lax"><span style="color:#000">用户主动通话频数：电话+智能外呼</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">active_calls_pct</span></td>
    <td class="tg-0lax"><span style="color:#000">用户主动通话占比 = 用户主动通话频数 / 通话频数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">chats_freq</span></td>
    <td class="tg-0lax"><span style="color:#000">聊天频数：企微聊天+在线客服</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">chats_freq_daily_avg</span></td>
    <td class="tg-0lax"><span style="color:#000">每日平均聊天频数：企微聊天+在线客服</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">active_chats_freq</span></td>
    <td class="tg-0lax"><span style="color:#000">用户主动聊天频数：企微聊天+在线客服</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">active_chats_pct</span></td>
    <td class="tg-0lax"><span style="color:#000">用户主动聊天占比 = 用户主动聊天频数 / 聊天频数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">vac_mention_num</span></td>
    <td class="tg-0lax"><span style="color:#000">沟通过程中度假产品的总提及次数</span></td>
  </tr>
  <tr>
    <td class="tg-0lax"><span style="color:#000">single_mention_num</span></td>
    <td class="tg-0lax"><span style="color:#000">沟通过程中度假产品的总提及次数</span></td>
  </tr>
</tbody>
</table>

<br>



## <font color = 'blue' >三、随机森林分类预测</font>
### 1. 数据预处理
``` python
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
```

```
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 969391 entries, 0 to 969390
Data columns (total 84 columns):
 #   Column                           Non-Null Count   Dtype   
---  ------                           --------------   -----   
 0   cust_id                          969391 non-null  int64   
 1   create_date                      969391 non-null  object  
 2   executed_flag_7                  969391 non-null  category
 3   executed_flag_15                 969391 non-null  category
 4   ordered_flag_7                   969391 non-null  category
 5   ordered_flag_15                  969391 non-null  category
 6   cust_level_1                     969391 non-null  int64   
 7   cust_level_2                     969391 non-null  int64   
 8   cust_level_3                     969391 non-null  int64   
 9   cust_level_4                     969391 non-null  int64   
 10  cust_level_5                     969391 non-null  int64   
 11  cust_level_6                     969391 non-null  int64   
 12  cust_level_7                     969391 non-null  int64   
 13  cust_type_1                      969391 non-null  int64   
 14  cust_type_2                      969391 non-null  int64   
 15  cust_type_3                      969391 non-null  int64   
 16  cust_type_4                      969391 non-null  int64   
 17  cust_group_type_1                969391 non-null  int64   
 18  cust_group_type_2                969391 non-null  int64   
 19  cust_group_type_3                969391 non-null  int64   
 20  is_office_weixin_1               969391 non-null  int64   
 21  sex_1.0                          969391 non-null  int64   
 22  age_group_2.0                    969391 non-null  int64   
 23  age_group_3.0                    969391 non-null  int64   
 24  age_group_4.0                    969391 non-null  int64   
 25  age_group_5.0                    969391 non-null  int64   
 26  prd_comp_grade_level_3.0         969391 non-null  int64   
 27  rg_time                          969391 non-null  float64 
 28  office_weixin_time               969391 non-null  float64 
 29  age                              969391 non-null  float64 
 30  access_channel_cnt               969391 non-null  float64 
 31  executed_orders_cnt              969391 non-null  float64 
 32  complaint_orders_cnt             969391 non-null  float64 
 33  cp_avg                           969391 non-null  float64 
 34  cp_min                           969391 non-null  float64 
 35  cp_max                           969391 non-null  float64 
 36  satisfaction_avg                 969391 non-null  float64 
 37  vac_executed_cnt                 969391 non-null  float64 
 38  vac_cp_avg                       969391 non-null  float64 
 39  vac_satisfaction_avg             969391 non-null  float64 
 40  decision_time                    969391 non-null  float64 
 41  browsing_days                    969391 non-null  float64 
 42  pv                               969391 non-null  float64 
 43  pv_daily_avg                     969391 non-null  float64 
 44  browsing_time                    969391 non-null  float64 
 45  browsing_time_max                969391 non-null  float64 
 46  browsing_time_daily_avg          969391 non-null  float64 
 47  browsing_products_cnt            969391 non-null  float64 
 48  browsing_products_cnt_daily_avg  969391 non-null  float64 
 49  to_booking_pages_cnt             969391 non-null  float64 
 50  search_times                     969391 non-null  float64 
 51  vac_search_times                 969391 non-null  float64 
 52  lowest_price_avg                 969391 non-null  float64 
 53  vac_lowest_price_avg             969391 non-null  float64 
 54  browsing_vac_prd_cnt             969391 non-null  float64 
 55  browsing_single_prd_cnt          969391 non-null  float64 
 56  collect_coupons_cnt              969391 non-null  float64 
 57  prd_satisfaction                 969391 non-null  float64 
 58  prd_remark_amount                969391 non-null  float64 
 59  prd_essence_amount               969391 non-null  float64 
 60  prd_coupon_amount                969391 non-null  float64 
 61  prd_money_amount                 969391 non-null  float64 
 62  prd_photo_amount                 969391 non-null  float64 
 63  comm_days                        969391 non-null  float64 
 64  comm_freq                        969391 non-null  float64 
 65  comm_freq_daily_avg              969391 non-null  float64 
 66  call_pct                         969391 non-null  float64 
 67  robot_pct                        969391 non-null  float64 
 68  officewx_pct                     969391 non-null  float64 
 69  chat_pct                         969391 non-null  float64 
 70  channels_cnt                     969391 non-null  float64 
 71  comm_time                        969391 non-null  float64 
 72  comm_time_daily_avg              969391 non-null  float64 
 73  call_completing_rate             969391 non-null  float64 
 74  calls_freq                       969391 non-null  float64 
 75  calls_freq_daily_avg             969391 non-null  float64 
 76  active_calls_freq                969391 non-null  float64 
 77  active_calls_pct                 969391 non-null  float64 
 78  chats_freq                       969391 non-null  float64 
 79  chats_freq_daily_avg             969391 non-null  float64 
 80  active_chats_freq                969391 non-null  float64 
 81  active_chats_pct                 969391 non-null  float64 
 82  vac_mention_num                  969391 non-null  float64 
 83  single_mention_num               969391 non-null  float64 
dtypes: category(4), float64(57), int64(22), object(1)
```


### 2. 数据重采样
&emsp;&emsp;数据总量为969391，其中成交样本量为2319，未成交样本量为967076，未成交 : 成交 ≈ 417 : 1，正负样本分布极不平衡，故考虑对数据进行重采样。
&emsp;&emsp;基于SMOTE算法，使用过采样与降采样相结合的方式，进行数据重采样。重采样后的数据分布为：未成交 : 成交 = 5 : 3。
``` python
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
```

```
Original dataset shape Counter({0: 967067, 1: 2319})
Resampled dataset shape Counter({0: 322355, 1: 193413})
```


### 3. 随机森林建模
&emsp;&emsp;基于随机森林，对处理后的数据建进分类预测。
``` python
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
```

<!--
<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-baqh{text-align:center;vertical-align:top}
.tg .tg-t4dz{background-color:#B0D4CC;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-amwm{font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-v658{background-color:#72B7EF;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-zm63{background-color:#B7DCFB;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-50tr{background-color:#D8ECE7;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-0lax{text-align:left;vertical-align:top}
</style>
-->
<table class="tg" style="undefined;table-layout: fixed; width: 268px">
<colgroup>
<col style="width: 66px">
<col style="width: 65px">
<col style="width: 69px">
<col style="width: 68px">
</colgroup>
<thead>
  <tr>
    <th class="tg-amwm" colspan="2" rowspan="2"><span style="font-weight:bold">混淆矩阵</span></th>
    <th class="tg-v658" colspan="2"><span style="font-weight:bold">预测值</span></th>
  </tr>
  <tr>
    <th class="tg-zm63"><span style="font-weight:bold">0</span></th>
    <th class="tg-zm63"><span style="font-weight:bold">1</span></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-t4dz" rowspan="2"><span style="font-weight:bold">真实值</span></td>
    <td class="tg-50tr"><span style="font-weight:bold">0</span></td>
    <td class="tg-baqh">64521</td>
    <td class="tg-baqh">74</td>
  </tr>
  <tr>
    <td class="tg-50tr"><span style="font-weight:bold">1</span></td>
    <td class="tg-baqh">244</td>
    <td class="tg-baqh">38315</td>
  </tr>
</tbody>
</table>

<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131732895.png" alt="模型评价指标结果" width="500" />
  <p>模型评价指标结果</p>
</div> 
&emsp;&emsp;根据重采样后的数据建立随机森林模型，模型在测试集上的AUC值为0.9963，预测准确率为100%，精确率为100%，召回率为99%，F1值为1.00。

<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131733499.png" alt="模型特征重要性排序" width="600" />
</div> 
&emsp;&emsp;将特征对模型的重要性进行降序排序。上图显示了每个特征对于该模型的预测能力的贡献程度。通常来说，具有较高重要性分数的特征对模型的预测能力有更大的影响。


### 4. 模型测试
&emsp;&emsp;**取数：** 基于上述导数逻辑，提取2023-08-02有浏览或沟通记录的用户的相关数据；
&emsp;&emsp;**目的：** 预测该部分用户在未来7天内是否有度假产品的成交订单。
``` sql
-- 查询2023.07有浏览或沟通记录的用户ID
drop table if exists tmp_cust_id_20230802;
create table tmp_cust_id_20230802 as
select cust_id, cast('2023-08-02' as date) create_date
from (
    select distinct cast(vt_mapuid as bigint) cust_id
    from dw.kn1_traf_app_day_detail   -- APP流量
    where dt = '20230802' and to_date(operate_time) = '2023-08-02' -- 过滤脏数据
    union
    select distinct cast(vt_mapuid as bigint) cust_id 
    from dw.kn1_traf_day_detail       -- PC/M站流量
    where dt = '20230802' and to_date(operate_time) = '2023-08-02' -- 过滤脏数据
    union
    select distinct b.user_id cust_id
    from (
        select case when length(cust_tel_no) > 11 then substr(cust_tel_no, -11) else cust_tel_no end tel_num
        from dw.kn1_call_tel_order_detail           -- 电话呼入呼出
        where dt = '20230802' and status = '通话' and to_date(status_start_time) = '2023-08-02'
    ) a
    left join (
        select user_id, feature_value as tel_num from dw.ol_rs_meta_feature_base_information
        where three_class_name = '手机号'
    ) b on b.tel_num = a.tel_num and a.tel_num is not null
    union
    select distinct cust_id
    from dw.kn1_sms_robot_outbound_call_detail      -- 智能外呼
    where dt = '20230802' and answer_flag = 1 and to_date(start_time) = '2023-08-02'
    union
    select distinct cust_id
    from dw.kn1_officeweixin_sender_cust_content    -- 企微聊天
    where dt = '20230802' and to_date(msg_time) = '2023-08-02'
    union
    select distinct cust_id
    from dw.kn1_autotask_user_acsessed_chat         -- 在线客服
    where dt = '20230802' and to_date(create_start_time) = '2023-08-02'
) t 
where cust_id in (
    select distinct cust_id from dw.kn1_usr_cust_attribute 
    where is_inside_staff = 0 and is_distribution = 0 and is_scalper = 0    -- 剔除内部会员/分销/黄牛
)   -- 未注销会员用户
    and cust_id between 1 and 1000000001
group by cust_id;
```
``` sql
-- 会员最后一次浏览/沟通后未来n天内的订单情况
drop table if exists tmp_cust_order_20230802;
create table tmp_cust_order_20230802 as
select 
    a.cust_id,                                          -- 会员ID
    a.create_date,                                      -- 会员最后一次浏览/沟通日期
    case when max(case when b.executed_date between date_add(a.create_date,1) and date_add(a.create_date,7) and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end executed_flag_7,              -- 会员最后一次浏览/沟通后未来7天内是否有度假产品的成交订单 0-否 1-是
    case when max(case when b.executed_date between date_add(a.create_date,1) and date_add(a.create_date,15) and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end executed_flag_15,             -- 会员最后一次浏览/沟通后未来15天内是否有度假产品的成交订单 0-否 1-是
    case when max(case when b.create_time between date_add(a.create_date,1) and date_add(a.create_date,7) and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end ordered_flag_7,               -- 会员最后一次浏览/沟通后未来7天内是否下单度假产品 0-否 1-是
    case when max(case when b.create_time between date_add(a.create_date,1) and date_add(a.create_date,15) and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end ordered_flag_15               -- 会员最后一次浏览/沟通后未来7天内是否下单度假产品 0-否 1-是
from tmp_cust_id_20230802 a
left join (
    select 
        cust_id, route_id, book_city,
        to_date(create_time) create_time,
        if(cancel_flag=0 and sign_flag=1 and cancel_sign_flag=0, sign_date, null) executed_date
    from dw.kn2_ord_order_detail_all
    where dt = '20230817' and create_time >= '2023-08-03'   -- 统计7月份的订单
        and valid_flag=1 and is_sub = 0 and distribution_flag in (0, 3, 4, 5)
) b on b.cust_id = a.cust_id
left join (
    select 
        distinct route_id, book_city,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) c on c.route_id = b.route_id and c.book_city = b.book_city
group by a.cust_id, a.create_date;
```
``` sql
-- 用户特征
drop table if exists tmp_cust_feature_20230802;
create table tmp_cust_feature_20230802 as
select
    a.cust_id,                                                                  -- 会员ID
    a.create_date,                                                              -- 最后一次浏览/沟通日期
    b.cust_level,                                                               -- 会员星级
    b.cust_type,                                                                -- 会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他
    b.cust_group_type,                                                          -- 会员类型（1：普通散客；2：团队成员；3：团队会员）
    b.is_office_weixin,                                                         -- 是否企业微信用户:0否 1是
    if(a.create_date>b.rg_time, datediff(a.create_date, b.rg_time)+1, 0) rg_time, -- 注册时间/天
    if(a.create_date>b.office_weixin_time and b.office_weixin_time is not null, datediff(a.create_date, b.office_weixin_time)+1, 0) office_weixin_time,-- 添加企业微信时间/天
    rs1.sex,                                                                    -- 用户性别
    rs2.age,                                                                    -- 用户年龄
    rs3.age_group,                                                              -- 用户年龄段
    rs4.tel_num,                                                                -- 用户手机号
    rs5.user_city,                                                              -- 用户所在城市
    rs6.user_province,                                                          -- 用户所在省份
    nvl(rs7.access_channel_cnt, 0) access_channel_cnt,                          -- 可触达渠道的个数
    rs7.access_channels                                                         -- 可触达渠道：push/企业微信/短信
from tmp_cust_id_20230802 a
left join (
    -- 会员属性
    select
        cust_id, cust_type, cust_level, to_date(rg_time) rg_time, cust_group_type, is_office_weixin, to_date(office_weixin_time) office_weixin_time
    from dw.kn1_usr_cust_attribute
    where cust_id in (select cust_id from tmp_cust_id_20230802)
) b on b.cust_id = a.cust_id
left join (
    -- 会员基本信息-性别
    select user_id, feature_name as sex
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '性别'
        and user_id in (select cust_id from tmp_cust_id_20230802)
) rs1 on a.cust_id = rs1.user_id
left join (
    -- 会员基本信息-年龄
    select user_id, feature_value as age
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '年龄'
        and user_id in (select cust_id from tmp_cust_id_20230802)
) rs2 on a.cust_id = rs2.user_id
left join (
    -- 会员基本信息-年龄段
    select user_id, feature_name as age_group
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '年龄段'
        and user_id in (select cust_id from tmp_cust_id_20230802)
) rs3 on a.cust_id = rs3.user_id
left join (
    -- 会员基本信息-手机号
    select user_id, feature_value as tel_num
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '手机号'
        and user_id in (select cust_id from tmp_cust_id_20230802)
) rs4 on a.cust_id = rs4.user_id
left join (
    -- 基本属性--基本信息-用户所在城市
    select user_id, feature_name as user_city, feature_value as user_city_key
    from dw.ol_rs_meta_feature_basic_info
    where three_class_name = '所在城市'
        and user_id in (select cust_id from tmp_cust_id_20230802)
) rs5 on a.cust_id = rs5.user_id
left join (
    -- 用户所在省份
    select city_key, parent_province_name as user_province
    from dw.kn1_pub_all_city_key
) rs6 on rs5.user_city_key = rs6.city_key
left join (
    -- 基本属性--个人信息-可触达渠道：push/企业微信/短信
    select 
        user_id, 
        count(1) as access_channel_cnt,
        concat_ws(',', collect_set(three_class_name)) as access_channels
    from dw.ol_rs_meta_feature_basic_info_access_channel
    where feature_value = 1 and user_id in (select cust_id from tmp_cust_id_20230802)
    group by user_id
) rs7 on a.cust_id = rs7.user_id;
```
``` sql
-- 计算用户历史59天内成交订单数、投诉订单数
drop table if exists tmp_cust_historical_order_20230802;
create table tmp_cust_historical_order_20230802 as
select
    a.cust_id,
    count(distinct case when b.executed_date between date_sub(a.create_date,59) and a.create_date then b.order_id else null end) executed_orders_cnt,                 -- 历史60天订单成交量
    count(distinct case when b.complaint_time between date_sub(a.create_date,59) and a.create_date then b.order_id else null end) complaint_orders_cnt,               -- 历史60天内订单投诉量
    nvl(avg(case when b.executed_date between date_sub(a.create_date,59) and a.create_date then b.travel_sign_amount else null end), 0) cp_avg,     -- 历史60天平均消费费用
    nvl(min(case when b.executed_date between date_sub(a.create_date,59) and a.create_date then b.travel_sign_amount else null end), 0) cp_min,     -- 历史60天最低消费费用
    nvl(max(case when b.executed_date between date_sub(a.create_date,59) and a.create_date then b.travel_sign_amount else null end), 0) cp_max,     -- 历史60天最高消费费用
    avg(case when b.return_visit_date between date_sub(a.create_date,59) and a.create_date then b.satisfaction else null end) satisfaction_avg,     -- 历史60天平均回访满意度评分
    count(distinct case when b.executed_date between date_sub(a.create_date,59) and a.create_date and c.producttype_class_name like '度假产品' then b.order_id else null end) vac_executed_cnt,   -- 历史60天内度假产品订单成交量
    nvl(avg(case when b.executed_date between date_sub(a.create_date,59) and a.create_date and c.producttype_class_name like '度假产品' then b.travel_sign_amount else null end), 0) vac_cp_avg,  -- 历史60天度假产品平均消费费用
    avg(case when b.return_visit_date between date_sub(a.create_date,59) and a.create_date and c.producttype_class_name like '度假产品' then b.satisfaction else null end) vac_satisfaction_avg   -- 历史60天度假产品平均回访满意度评分
from tmp_cust_id_20230802 a
left join (
    select 
        cust_id, order_id, 
        if(cancel_flag=0 and sign_flag=1 and cancel_sign_flag=0, sign_date, null) executed_date,    -- 订单是否成交 0-否 1-是
        to_date(complaint_time) complaint_time, -- 投诉时间
        travel_sign_amount,     -- 订单价格
        satisfaction,           -- 回访满意度总分
        return_visit_date,      -- 回访日期
        route_id, book_city
    from dw.kn2_ord_order_detail_all
    where dt = '20230802' and create_time >= date_sub('2023-08-02', 59)
        and valid_flag=1 and is_sub = 0 and distribution_flag in (0, 3, 4, 5)
) b on b.cust_id = a.cust_id
left join (
    select 
        distinct route_id, book_city,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) c on c.route_id = b.route_id and c.book_city = b.book_city
group by a.cust_id;
```
``` sql
--  用户的浏览行为信息
drop table if exists tmp_cust_browsing_info_20230802;
create table tmp_cust_browsing_info_20230802 as
select
    a.cust_id,                                              -- 会员ID
    a.create_date,                                          -- 用户最后一次浏览/沟通日期
    b.operate_date,                                         -- 用户访问日期
    -- if(b.operate_date between date_sub(a.create_date, 14) and a.create_date, 1, 0) browsing_flag, -- 最后一次浏览/沟通前15天是否进行浏览 0-否 1-是
    case when b.operate_date between date_sub(a.create_date, 14) and a.create_date then 1
        when b.operate_date < date_sub(a.create_date, 14) and b.operate_date is not null then 2
        else 0
    end browsing_flag,
    b.visitor_trace,                                        -- 访客标记
    b.residence_time,                                       -- 页面停留时间
    b.product_id,                                           -- 产品ID
    b.book_id,                                              -- 预订页ID
    b.search_key,                                           -- 搜索词
    b.search_key_type,                                      -- 搜索词类型 1-度假产品 0-单资源产品 null-无搜索词
    c.lowest_price,                                         -- 产品最低价
    d.producttype_class_name,                               -- 浏览产品大类
    nvl(e.collect_coupons_status, 0) collect_coupons_status,-- 优惠券领取状态 1-成功 0-未领取/失败
    f.f_satisfaction,                                       -- 产品平均满意度
    f.f_comp_grade_level,                                   -- 总评级别
    f.f_remark_amount,                                      -- 点评数目
    f.f_essence_amount,                                     -- 精华点评数目
    f.f_coupon_amount,                                      -- 返抵用券金额
    f.f_money_amount,                                       -- 返现金额
    f.f_photo_amount                                        -- 图片数目
from tmp_cust_id_20230802 a 
left join (
    -- 流量域
    select 
        distinct vt_mapuid, to_date(operate_time) operate_date, visitor_trace, residence_time, product_id, book_id, search_key, 
        case when search_key like '%票%' or search_key like '%酒店%' then 0 
            when search_key is null or lower(search_key) = 'null' then null
            else 1
        end search_key_type
    from dw.kn1_traf_app_day_detail
    where dt between '20230719' and '20230802'
        and vt_mapuid in (select cust_id from tmp_cust_id_20230802)
        and to_date(operate_time) between '2023-07-19' and '2023-08-02' -- 过滤脏数据
    union
    select 
        distinct vt_mapuid, to_date(operate_time) operate_date, visitor_trace, residence_time, product_id, book_id, search_key, 
        case when search_key like '%票%' or search_key like '%酒店%' then 0 
            when lower(search_key) is null or lower(search_key) = 'null' then null
            else 1
        end search_key_type
    from dw.kn1_traf_day_detail
    where dt between '20230719' and '20230802'
        and vt_mapuid in (select cust_id from tmp_cust_id_20230802)
        and to_date(operate_time) between '2023-07-19' and '2023-08-02'
) b on b.vt_mapuid = a.cust_id
left join (
    -- 产品最低价
    select distinct route_id, lowest_price
    from dw.kn1_prd_route
) c on c.route_id = b.product_id
left join (
    select 
        distinct route_id,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) d on d.route_id = b.product_id
left join (
    -- 用户领券情况
    select distinct cust_id, collect_coupons_status, to_date(operate_time) operate_date
    from dw.ods_crmmkt_mkt_scene_clear_intention_cust_transform
    where to_date(operate_time) between '2023-07-19' and '2023-08-02'
        and collect_coupons_status = 1 and cust_id in (select cust_id from tmp_cust_id_20230802)
) e on e.cust_id = b.vt_mapuid and e.operate_date = b.operate_date
left join (
    -- 获取产品关联的满意度和评价总人数
    select 
        t1.f_product_id, f_satisfaction / 1000000 f_satisfaction, f_comp_grade_level, f_remark_amount, 
        f_essence_amount, f_coupon_amount, f_money_amount, f_photo_amount
    from dw.ods_ncs_t_nremark_stat_product t1
    join (
        select f_product_id, max(f_update_time) max_update_time
        from dw.ods_ncs_t_nremark_stat_product
        group by f_product_id
    ) t2 ON t1.f_product_id = t2.f_product_id and t1.f_update_time = t2.max_update_time
) f on f.f_product_id = b.product_id;
```
``` sql
-- 用户的沟通行为信息
drop table if exists tmp_cust_communication_info_20230802;
create table tmp_cust_communication_info_20230802 as
select
    a.cust_id,                              -- 会员编号
    a.create_date,                          -- 用户最后一次浏览/沟通日期
    b.comm_date,                            -- 沟通日期
    -- if(b.comm_date between date_sub(a.create_date, 14) and a.create_date, 1, 0) comm_flag, -- 最后一次浏览/沟通前15天是否进行沟通 0-否 1-是
    case when b.comm_date between date_sub(a.create_date, 14) and a.create_date then 1
        when b.comm_date < date_sub(a.create_date, 14) and b.comm_date is not null then 2
        else 0
    end comm_flag,
    b.channel,                              -- 沟通渠道名称
    b.channel_id,                           -- 沟通渠道ID 1-电话呼入呼出 2-智能外呼 3-企微聊天 4-在线客服
    b.comm_duration,                        -- 沟通持续时长
    b.day_calls,                            -- 每日电话呼入呼出/智能外呼次数
    b.comm_num,                             -- 沟通量：通话量/聊天数
    b.active_comm_num,                      -- 用户主动进行沟通的数量
    b.vac_mention_num,                      -- 沟通过程中度假产品的提及次数
    b.single_mention_num                    -- 沟通过程中单资源产品的提及次数
from tmp_cust_id_20230802 a 
left join (
    -- 电话明细
    select
        t2.cust_id, t1.comm_date,
        sum(t1.status_time) comm_duration,  -- 通话总时长
        count(1) day_calls,                 -- 每日电话呼入/呼出次数
        sum(case when t1.status='通话' then 1 else 0 end) comm_num,        -- 接通量
        sum(case when t1.status='通话' and t1.calldir='呼入' then 1 else 0 end) active_comm_num,-- 用户呼入量
        sum(case when t4.producttype_class_name like '度假产品' then 1 else 0 end) vac_mention_num,
        sum(case when t4.producttype_class_name like '单资源产品' then 1 else 0 end) single_mention_num,
        '电话呼入呼出' as channel, 1 as channel_id
    from (
        select
            case when length(cust_tel_no) > 11 then substr(cust_tel_no, -11) else cust_tel_no end tel_num,
            to_date(status_start_time) comm_date,
            status_time, status, calldir, order_id
        from dw.kn1_call_tel_order_detail
        where dt between '20230719' and '20230802' and to_date(status_start_time) between '2023-07-19' and '2023-08-02' and tel_order_type in (0, 1)     -- 通话时间在下单时间之前
    ) t1 
    left join (select cust_id, tel_num from tmp_cust_feature) t2
    on t2.tel_num = t1.tel_num
    left join (
        select order_id, route_id, book_city from dw.kn2_ord_order_detail_all
        where dt = '20230802' and create_time >= '2023-07-01'
    ) t3 on t3.order_id = t1.order_id
    left join (
        select distinct route_id, book_city,
            case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
        from dw.kn2_dim_route_product_sale
    ) t4 on t4.route_id = t3.route_id and t4.book_city = t3.book_city
    group by cust_id, comm_date
    union all
    -- 机器人外呼明细
    select
        cust_id, to_date(start_time) comm_date,
        sum(call_time) comm_duration,           -- 通话总时长
        count(1) day_calls,                     -- 每日外呼次数
        sum(answer_flag) comm_num,              -- 接通量
        sum(case when answer_flag=1 and generate_task_is = 1 and lower(label) in ('a','b','q','c','p','s') then 1 else 0 end) active_comm_num,   -- 命中用户出游意向量
        0 as vac_mention_num,
        0 as single_mention_num,
        '智能外呼' as channel, 2 as channel_id
    from dw.kn1_sms_robot_outbound_call_detail
    where dt between '20230719' and '20230802' and to_date(start_time) between '2023-07-19' and '2023-08-02'
        and cust_id in (select cust_id from tmp_cust_id_20230802)
    group by cust_id, to_date(start_time)
    union all
    -- 企微聊天明细
    select 
        cust_id, to_date(msg_time) comm_date,
        null as comm_duration,                  -- 聊天时长：null
        0 as day_calls,
        count(msg_time) comm_num,               -- 发送消息数
        sum(case when type=1 then 1 else 0 end) active_comm_num,   -- 用户主动聊天数
        sum(case when contact like '%商旅%' or contact like '%跟团%' or contact like '%自驾%' or contact like '%自助%'
                or contact like '%目的地服务%' or contact like '%签证%' or contact like '%团队%' or contact like '%定制%'
                or contact like '%游轮%' or contact like '%旅拍%' or contact like '%游%' or contact like '%团%' then 1 else 0 end) vac_mention_num,
        sum(case when contact like '%火车票%' or contact like '%机票%' or contact like '%酒店%' or contact like '%百货%'
                or contact like '%用车服务%' or contact like '%高铁%' or contact like '%票%' or contact like '%硬座%'
                or contact like '%软卧%' or contact like '%卧铺%' or contact like '%航班%' then 1 else 0 end) single_mention_num,
        '企微聊天' as channel, 3 as channel_id
    from dw.kn1_officeweixin_sender_cust_content
    where dt between '20230719' and '20230802' and to_date(msg_time) between '2023-07-19' and '2023-08-02'
        and cust_id in (select cust_id from tmp_cust_id_20230802)
    group by cust_id, to_date(msg_time)
    union all
    -- 在线客服沟通明细
    select
        cust_id,
        to_date(create_start_time) comm_date,
        null as comm_duration,                  -- 聊天时长：null
        0 as day_calls,
        count(1) comm_num,                      -- 发送消息数
        sum(case when content like '%客人发送消息%' then 1 else 0 end) active_comm_num,    -- 用户主动发送消息数
        sum(case when content like '%商旅%' or content like '%跟团%' or content like '%自驾%' or content like '%自助%'
                or content like '%目的地服务%' or content like '%签证%' or content like '%团队%' or content like '%定制%'
                or content like '%游轮%' or content like '%旅拍%' or content like '%游%' or content like '%团%' then 1 else 0 end) vac_mention_num,
        sum(case when content like '%火车票%' or content like '%机票%' or content like '%酒店%' or content like '%百货%'
                or content like '%用车服务%' or content like '%高铁%' or content like '%票%' or content like '%硬座%'
                or content like '%软卧%' or content like '%卧铺%' or content like '%航班%' then 1 else 0 end) single_mention_num,
        '在线客服' as channel, 4 as channel_id
    from dw.kn1_autotask_user_acsessed_chat
    where dt between '20230719' and '20230802' and to_date(create_start_time) between '2023-07-19' and '2023-08-02'
        and cust_id in (select cust_id from tmp_cust_id_20230802)
    group by cust_id, to_date(create_start_time)
) b on b.cust_id = a.cust_id;
```
``` sql
-- 计算用户的行为特征——浏览特征 + 沟通特征
drop table if exists tmp_cust_behavior_info_20230802;
create table tmp_cust_behavior_info_20230802 as
select
    a.cust_id,                                                                                      -- 会员ID
    b.decision_time,                                                                                -- 决策时间
    nvl(c.browsing_days, 0) browsing_days,                                                          -- 历史浏览天数 
    nvl(c.pv, 0) pv,                                                                                -- 用户总pv
    nvl(c.pv_daily_avg, 0) pv_daily_avg,                                                            -- 每日平均pv
    nvl(c.browsing_time, 0) browsing_time,                                                          -- 用户总浏览时间
    nvl(c.browsing_time_max, 0) browsing_time_max,                                                  -- 单次浏览的最大浏览时间
    nvl(c.browsing_time_daily_avg, 0) browsing_time_daily_avg,                                      -- 每日平均浏览时间
    nvl(c.browsing_products_cnt, 0) browsing_products_cnt,                                          -- 历史浏览产品量
    nvl(c.browsing_products_cnt_daily_avg, 0) browsing_products_cnt_daily_avg,                      -- 每日平均浏览产品量
    nvl(c.to_booking_pages_cnt, 0) to_booking_pages_cnt,                                            -- 到预定页数
    nvl(c.search_times, 0) search_times,                                                            -- 搜索次数
    nvl(c.vac_search_times, 0) vac_search_times,                                                    -- 度假产品搜索次数
    c.lowest_price_avg,                                                                             -- 历史浏览产品（最低价）平均价
    c.vac_lowest_price_avg,                                                                         -- 历史浏览度假产品的平均最低价
    nvl(c.browsing_vac_prd_cnt, 0) browsing_vac_prd_cnt,                                            -- 历史浏览的度假产品数量
    nvl(c.browsing_single_prd_cnt, 0) browsing_single_prd_cnt,                                      -- 历史浏览的单资源产品数量
    nvl(c.collect_coupons_cnt, 0) collect_coupons_cnt,                                              -- 优惠券领券个数
    c.prd_satisfaction,                                                                             -- 平均产品满意度
    c.prd_comp_grade_level,                                                                         -- 最大产品总评级别
    nvl(c.prd_remark_amount, 0) prd_remark_amount,                                                  -- 平均点评数目
    nvl(c.prd_essence_amount, 0) prd_essence_amount,                                                -- 平均精华点评数目
    nvl(c.prd_coupon_amount, 0) prd_coupon_amount,                                                  -- 平均返抵用券金额
    nvl(c.prd_money_amount, 0) prd_money_amount,                                                    -- 平均返现金额
    nvl(c.prd_photo_amount, 0) prd_photo_amount,                                                    -- 平均音频数目
    nvl(d.comm_days, 0) comm_days,                                                                  -- 历史沟通天数
    nvl(d.comm_freq, 0) comm_freq,                                                                  -- 总沟通次数（count(channel)）
    nvl(d.comm_freq_daily_avg, 0) comm_freq_daily_avg,                                              -- 每日平均沟通次数
    nvl(d.call_pct, 0) call_pct,                                                                    -- 电话呼入呼出占比（电话呼入呼出/总沟通次数）
    nvl(d.robot_pct, 0) robot_pct,                                                                  -- 智能外呼占比
    nvl(d.officewx_pct, 0) officewx_pct,                                                            -- 企微聊天占比
    nvl(d.chat_pct, 0) chat_pct,                                                                    -- 在线客服占比
    nvl(d.channels_cnt, 0) as channels_cnt,                                                         -- 历史沟通渠道数（count(distinct channel)）
    nvl(d.comm_time, 0) comm_time,                                                                  -- 总沟通时长（特指电话、智能外呼）
    nvl(d.comm_time_daily_avg, 0) comm_time_daily_avg,                                              -- 每日平均沟通时长
    nvl(d.call_completing_rate, 0) call_completing_rate,                                            -- 电话接通率（特指电话、智能外呼）
    nvl(d.calls_freq, 0) calls_freq,                                                                -- 通话频数：电话+智能外呼
    nvl(d.calls_freq_daily_avg, 0) calls_freq_daily_avg,                                            -- 每日平均通话频数：电话+智能外呼
    nvl(d.active_calls_freq, 0) active_calls_freq,                                                  -- 用户主动通话频数：电话+智能外呼
    nvl(d.active_calls_pct, 0) active_calls_pct,                                                    -- 用户主动通话占比 = 用户主动通话频数 / 通话频数
    nvl(d.chats_freq, 0) chats_freq,                                                                -- 聊天频数：企微聊天+在线客服
    nvl(d.chats_freq_daily_avg, 0) chats_freq_daily_avg,                                            -- 每日平均聊天频数：企微聊天+在线客服
    nvl(d.active_chats_freq, 0) active_chats_freq,                                                  -- 用户主动聊天频数：企微聊天+在线客服
    nvl(d.active_chats_pct, 0) active_chats_pct,                                                    -- 用户主动聊天占比 = 用户主动聊天频数 / 聊天频数
    nvl(d.vac_mention_num, 0) vac_mention_num,                                                      -- 沟通过程中度假产品的总提及次数
    nvl(d.single_mention_num, 0) single_mention_num                                                 -- 沟通过程中度假产品的总提及次数
from tmp_cust_id_20230802 a
left join (
    -- 计算决策时间
    select 
        t1.cust_id,
        case when max(browsing_flag)=2 or max(comm_flag)=2 then 16
            when max(browsing_flag)=1 or max(comm_flag)=1 then max(datediff(create_date, least(nvl(operate_date,create_date), nvl(comm_date,create_date)))) + 1
            else 0
        end decision_time
    from (select cust_id, create_date, operate_date, browsing_flag from tmp_cust_browsing_info_20230802) t1
    left join (select cust_id, comm_date, comm_flag from tmp_cust_communication_info_20230802) t2
    on t1.cust_id = t2.cust_id
    group by t1.cust_id
) b on b.cust_id = a.cust_id
left join (
    select  
        cust_id,
        min(operate_date) first_operate_date,
        count(distinct operate_date) browsing_days,
        count(visitor_trace) pv,
        round(count(visitor_trace) / count(distinct operate_date), 4) pv_daily_avg,
        sum(residence_time) browsing_time,
        max(residence_time) browsing_time_max,
        round(sum(residence_time) / count(distinct operate_date), 4) browsing_time_daily_avg,
        count(distinct product_id) browsing_products_cnt,
        round(count(distinct product_id) / count(distinct operate_date), 4) browsing_products_cnt_daily_avg,
        count(book_id) to_booking_pages_cnt,
        count(search_key) search_times,
        sum(search_key_type) vac_search_times,
        round(avg(lowest_price), 4) lowest_price_avg,
        round(avg(case when producttype_class_name like '度假产品' then lowest_price else null end), 4) vac_lowest_price_avg,
        count(distinct case when producttype_class_name like '度假产品' then product_id else null end) browsing_vac_prd_cnt,
        count(distinct case when producttype_class_name like '单资源产品' then product_id else null end) browsing_single_prd_cnt,
        count(distinct case when collect_coupons_status = 1 then operate_date else null end) collect_coupons_cnt,
        round(avg(f_satisfaction), 4) prd_satisfaction,
        max(f_comp_grade_level) prd_comp_grade_level,
        round(avg(f_remark_amount), 4) prd_remark_amount,
        round(avg(f_essence_amount), 4) prd_essence_amount,
        round(avg(f_coupon_amount), 4) prd_coupon_amount,
        round(avg(f_money_amount), 4) prd_money_amount,
        round(avg(f_photo_amount), 4) prd_photo_amount
    from tmp_cust_browsing_info_20230802
    where browsing_flag = 1         -- 只对有浏览行为的用户计算指标
    group by cust_id
) c on c.cust_id = a.cust_id
left join (
    select  
        cust_id,
        min(comm_date) first_comm_date,
        count(distinct comm_date) comm_days,
        count(channel_id) comm_freq,
        round(count(channel_id)/count(distinct comm_date), 4) comm_freq_daily_avg,
        round(sum(if(channel_id=1,1,0)) / count(channel_id) * 100, 4) call_pct,
        round(sum(if(channel_id=2,1,0)) / count(channel_id) * 100, 4) robot_pct,
        round(sum(if(channel_id=3,1,0)) / count(channel_id) * 100, 4) officewx_pct,
        round(sum(if(channel_id=4,1,0)) / count(channel_id) * 100, 4) chat_pct,
        count(distinct channel_id) channels_cnt,
        sum(comm_duration) comm_time,
        round(sum(comm_duration) / count(distinct comm_date), 4) comm_time_daily_avg,
        if(sum(day_calls)<>0, round(sum(if(channel_id in (1,2), comm_num, 0)) / sum(day_calls) * 100, 4), 0) call_completing_rate,
        sum(if(channel_id in (1,2), comm_num, 0)) calls_freq,
        round(sum(if(channel_id in (1,2), comm_num, 0)) / count(distinct if(channel_id in (1,2), comm_date, null)), 4) calls_freq_daily_avg,
        sum(if(channel_id in (1,2), active_comm_num, 0)) active_calls_freq,
        round(sum(if(channel_id in (1,2), active_comm_num, 0)) / sum(if(channel_id in (1,2), comm_num, 0)) * 100, 4) active_calls_pct,
        sum(if(channel_id in (3,4), comm_num, 0)) chats_freq,
        round(sum(if(channel_id in (3,4), comm_num, 0)) / count(distinct if(channel_id in (3,4), comm_date, null)), 4) chats_freq_daily_avg,
        sum(if(channel_id in (3,4), active_comm_num, 0)) active_chats_freq,
        round(sum(if(channel_id in (3,4), active_comm_num, 0)) / sum(if(channel_id in (3,4), comm_num, 0)) * 100, 4) active_chats_pct,
        sum(vac_mention_num) vac_mention_num,
        sum(single_mention_num) single_mention_num
    from tmp_cust_communication_info_20230802
    where comm_flag = 1         -- 只对有沟通行为的用户计算指标
    group by cust_id
) d on d.cust_id = a.cust_id;
```
``` sql
-- 汇总以上表
drop table if exists tmp_cust_order_pred_20230802;
create table tmp_cust_order_pred_20230802 as
select
    t1.cust_id,                                                 -- 会员ID
    t1.create_date,                                             -- 会员最后一次浏览/沟通日期
    t1.executed_flag_7,                                         -- 会员最后一次浏览/沟通后未来7天内是否有度假产品的成交订单 0-否 1-是
    t1.executed_flag_15,                                        -- 会员最后一次浏览/沟通后未来15天内是否有度假产品的成交订单 0-否 1-是
    t1.ordered_flag_7,                                          -- 会员最后一次浏览/沟通后未来7天内是否下单度假产品 0-否 1-是
    t1.ordered_flag_15,                                         -- 会员最后一次浏览/沟通后未来15天内是否下单度假产品 0-否 1-是
    t2.cust_level,                                              -- 会员星级
    t2.cust_type,                                               -- 会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他
    t2.cust_group_type,                                         -- 会员类型（1：普通散客；2：团队成员；3：团队会员）
    t2.is_office_weixin,                                        -- 是否企业微信用户:0否 1是
    t2.rg_time,                                                 -- 注册时间/天
    t2.office_weixin_time,                                      -- 添加企业微信时间/天
    t2.sex,                                                     -- 用户性别
    t2.age,                                                     -- 用户年龄
    t2.age_group,                                               -- 用户年龄段
    t2.access_channel_cnt,                                      -- 可触达渠道的个数
    t3.executed_orders_cnt,                                     -- 历史60天订单成交量
    t3.complaint_orders_cnt,                                    -- 历史60天内订单投诉量
    t3.cp_avg,                                                  -- 历史60天平均消费费用
    t3.cp_min,                                                  -- 历史60天最低消费费用
    t3.cp_max,                                                  -- 历史60天最高消费费用
    t3.satisfaction_avg,                                        -- 历史60天平均回访满意度评分
    t3.vac_executed_cnt,                                        -- 历史60天内度假产品订单成交量
    t3.vac_cp_avg,                                              -- 历史60天度假产品平均消费费用
    t3.vac_satisfaction_avg,                                    -- 历史60天度假产品平均回访满意度评分
    t4.decision_time,                                           -- 决策时间
    t4.browsing_days,                                           -- 历史浏览天数 
    t4.pv,                                                      -- 用户总pv
    t4.pv_daily_avg,                                            -- 每日平均pv
    t4.browsing_time,                                           -- 用户总浏览时间
    t4.browsing_time_max,                                       -- 单次浏览的最大浏览时间
    t4.browsing_time_daily_avg,                                 -- 每日平均浏览时间
    t4.browsing_products_cnt,                                   -- 历史浏览产品量
    t4.browsing_products_cnt_daily_avg,                         -- 每日平均浏览产品量
    t4.to_booking_pages_cnt,                                    -- 到预定页数
    t4.search_times,                                            -- 搜索次数
    t4.vac_search_times,                                        -- 度假产品搜索次数
    t4.lowest_price_avg,                                        -- 历史浏览产品（最低价）平均价
    t4.vac_lowest_price_avg,                                    -- 历史浏览度假产品的平均最低价
    t4.browsing_vac_prd_cnt,                                    -- 历史浏览的度假产品数量
    t4.browsing_single_prd_cnt,                                 -- 历史浏览的单资源产品数量
    t4.collect_coupons_cnt,                                     -- 优惠券领券个数
    t4.prd_satisfaction,                                        -- 平均产品满意度
    t4.prd_comp_grade_level,                                    -- 最大产品总评级别
    t4.prd_remark_amount,                                       -- 平均点评数目
    t4.prd_essence_amount,                                      -- 平均精华点评数目
    t4.prd_coupon_amount,                                       -- 平均返抵用券金额
    t4.prd_money_amount,                                        -- 平均返现金额
    t4.prd_photo_amount,                                        -- 平均音频数目
    t4.comm_days,                                               -- 历史沟通天数
    t4.comm_freq,                                               -- 总沟通次数（count(channel)）
    t4.comm_freq_daily_avg,                                     -- 每日平均沟通次数
    t4.call_pct,                                                -- 电话呼入呼出占比（电话呼入呼出/总沟通次数）
    t4.robot_pct,                                               -- 智能外呼占比
    t4.officewx_pct,                                            -- 企微聊天占比
    t4.chat_pct,                                                -- 在线客服占比
    t4.channels_cnt,                                            -- 历史沟通渠道数（count(distinct channel)）
    t4.comm_time,                                               -- 总沟通时长（特指电话、智能外呼）
    t4.comm_time_daily_avg,                                     -- 每日平均沟通时长
    t4.call_completing_rate,                                    -- 电话接通率（特指电话、智能外呼）
    t4.calls_freq,                                              -- 通话频数：电话+智能外呼
    t4.calls_freq_daily_avg,                                    -- 每日平均通话频数：电话+智能外呼
    t4.active_calls_freq,                                       -- 用户主动通话频数：电话+智能外呼
    t4.active_calls_pct,                                        -- 用户主动通话占比 = 用户主动通话频数 / 通话频数
    t4.chats_freq,                                              -- 聊天频数：企微聊天+在线客服
    t4.chats_freq_daily_avg,                                    -- 每日平均聊天频数：企微聊天+在线客服
    t4.active_chats_freq,                                       -- 用户主动聊天频数：企微聊天+在线客服
    t4.active_chats_pct,                                        -- 用户主动聊天占比 = 用户主动聊天频数 / 聊天频数
    t4.vac_mention_num,                                         -- 沟通过程中度假产品的总提及次数
    t4.single_mention_num                                       -- 沟通过程中度假产品的总提及次数
from tmp_cust_order_20230802 t1
left join tmp_cust_feature_20230802 t2
on t2.cust_id = t1.cust_id
left join tmp_cust_historical_order_20230802 t3
on t3.cust_id = t1.cust_id
left join tmp_cust_behavior_info_20230802 t4
on t4.cust_id = t1.cust_id;
```
<br>

&emsp;&emsp;基于上述建立的随机森林模型，对2023-08-02有浏览或沟通记录的用户在未来7天内度假产品的成交情况进行预测，预测结果如下。
``` python
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
```

<!-- 
<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-baqh{text-align:center;vertical-align:top}
.tg .tg-t4dz{background-color:#B0D4CC;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-amwm{font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-v658{background-color:#72B7EF;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-zm63{background-color:#B7DCFB;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-50tr{background-color:#D8ECE7;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-0lax{text-align:left;vertical-align:top}
</style> 
-->
<table class="tg" style="undefined;table-layout: fixed; width: 568px">
<colgroup>
<col style="width: 95px">
<col style="width: 95px">
<col style="width: 65px">
<col style="width: 65px">
<col style="width: 65px">
<col style="width: 65px">
<col style="width: 65px">
<col style="width: 65px">
</colgroup>
<thead>
  <tr>
    <th class="tg-amwm" colspan="2" rowspan="3"><span style="font-weight:bold">混淆矩阵</span></th>
    <th class="tg-v658" colspan="6"><span style="font-weight:bold">预测值</span></th>
  </tr>
  <tr>
    <th class="tg-v658" colspan="2"><span style="font-weight:bold">阈值=0.5</span></th>
    <th class="tg-v658" colspan="2"><span style="font-weight:bold">阈值=0.4</span></th>
    <th class="tg-v658" colspan="2"><span style="font-weight:bold">阈值=0.3</span></th>
  </tr>
  <tr>
    <th class="tg-zm63"><span style="font-weight:bold">0</span></th>
    <th class="tg-zm63"><span style="font-weight:bold">1</span></th>
    <th class="tg-zm63"><span style="font-weight:bold">0</span></th>
    <th class="tg-zm63"><span style="font-weight:bold">1</span></th>
    <th class="tg-zm63"><span style="font-weight:bold">0</span></th>
    <th class="tg-zm63"><span style="font-weight:bold">1</span></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-t4dz" rowspan="2"><span style="font-weight:bold">真实值</span></td>
    <td class="tg-50tr"><span style="font-weight:bold">0（93664）</span></td>
    <td class="tg-baqh">47754</td>
    <td class="tg-baqh">45910</td>
    <td class="tg-baqh">19598</td>
    <td class="tg-baqh">74066</td>
    <td class="tg-baqh">6659</td>
    <td class="tg-baqh">87005</td>
  </tr>
  <tr>
    <td class="tg-50tr"><span style="font-weight:bold">1（1391）</span></td>
    <td class="tg-baqh">579</td>
    <td class="tg-baqh">812</td>
    <td class="tg-baqh">243</td>
    <td class="tg-baqh">1148</td>
    <td class="tg-baqh">82</td>
    <td class="tg-baqh">1309</td>
  </tr>
</tbody>
</table>

<!-- 
<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-vjyh{background-color:#E6E7E7;border-color:inherit;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-c3ow{border-color:inherit;text-align:center;vertical-align:top}
</style>
-->
<table class="tg" style="undefined;table-layout: fixed; width: 379px">
<colgroup>
<col style="width: 166px">
<col style="width: 71px">
<col style="width: 71px">
<col style="width: 71px">
</colgroup>
<thead>
  <tr>
    <th class="tg-vjyh"><span style="font-weight:bold">评价指标\预测阈值</span></th>
    <th class="tg-vjyh"><span style="font-weight:bold">阈值=0.5</span></th>
    <th class="tg-vjyh"><span style="font-weight:bold">阈值=0.4</span></th>
    <th class="tg-vjyh"><span style="font-weight:bold">阈值=0.3</span></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-vjyh"><span style="font-weight:bold">AUC</span></td>
    <td class="tg-c3ow">0.5468</td>
    <td class="tg-c3ow">0.5173</td>
    <td class="tg-c3ow">0.5061</td>
  </tr>
  <tr>
    <td class="tg-vjyh"><span style="font-weight:bold">accuracy</span></td>
    <td class="tg-c3ow">0.51</td>
    <td class="tg-c3ow">0.22</td>
    <td class="tg-c3ow">0.08</td>
  </tr>
  <tr>
    <td class="tg-vjyh"><span style="font-weight:bold">precision</span></td>
    <td class="tg-c3ow">0.02</td>
    <td class="tg-c3ow">0.02</td>
    <td class="tg-c3ow">0.01</td>
  </tr>
  <tr>
    <td class="tg-vjyh"><span style="font-weight:bold">recall</span></td>
    <td class="tg-c3ow">0.58</td>
    <td class="tg-c3ow">0.83</td>
    <td class="tg-c3ow">0.94</td>
  </tr>
  <tr>
    <td class="tg-vjyh"><span style="font-weight:bold">f1-score</span></td>
    <td class="tg-c3ow">0.03</td>
    <td class="tg-c3ow">0.03</td>
    <td class="tg-c3ow">0.03</td>
  </tr>
</tbody>
</table>

&emsp;&emsp;对2023-08-02有浏览或沟通记录的用户，利用训练好的随机森林模型对其未来7天内度假产品的成交情况进行预测。不同预测阈值下，预测值与真实值之间的混淆矩阵和评价指标结果见上表。

<br>

