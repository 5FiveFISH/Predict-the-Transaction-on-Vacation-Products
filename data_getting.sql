--------------------------------------------------------------------建模数据--------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
-- 所有用户的平均订单成交时间间隔：58.9天
WITH ExecutedOrders AS (
    SELECT
        cust_id,
        order_id,
        executed_date
    FROM (
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------





--------------------------------------------------------------------测试数据--------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------

