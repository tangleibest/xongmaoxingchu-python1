import datetime
import time
from datetime import datetime, date, timedelta
import datetime
import pandas as pd

import pymysql
from DBUtils.PooledDB import PooledDB

pool_project = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei',
                        passwd='tanglei', db='commerce',
                        port=3306)

pool = PooledDB(pymysql, 5, host='192.168.31.126', user='root',
                passwd='123456', db='finebi',
                port=3306)


# 获前几个月第一天
def getTheMonth(n, strf):
    date = datetime.datetime.today()
    month = date.month
    year = date.year
    for i in range(n):
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
    return datetime.date(year, month, 1).strftime(strf)


# 根据开始日期、结束日期返回这段时间里所有天的集合
def getDatesByTimes(sDateStr, eDateStr):
    list = []
    datestart = datetime.datetime.strptime(sDateStr, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(eDateStr, '%Y-%m-%d')
    list.append(datestart.strftime('%Y-%m-%d'))
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        list.append(datestart.strftime('%Y-%m-%d'))
    return list


# 插入每天熊猫星厨业务数据看板历史数据
def insert_data():
    db = pool_project.connection()
    cur = db.cursor()
    db2 = pool.connection()
    cur2 = db2.cursor()
    sql = "select " \
          "case p.area_id " \
          "when 12 then '北京' " \
          "when 14 then '上海' " \
          "when 17 then '深圳' " \
          "when 21 then '杭州' " \
          "else p.area_id end city, " \
          "p.project_name , s.stalls_name , s.entry_fee ,s.monthly_rent  , p.business_status  ,COUNT(*) stall_count " \
          "from stalls s left join project p " \
          "on s.project_id = p.project_id and s.is_delete = 0 and p.is_delete = 0 " \
          "where p.project_id is not null " \
          "and p.business_status=0  and p.area_id !=17 " \
          "GROUP BY p.area_id,p.project_name order by p.area_id,p.project_id"
    cur.execute(sql)
    results = cur.fetchall()

    # [项目数，档口数,新签数，续签数,退场,动销数,入住档口,付款档口,档口入住率,
    # 档口招商率,当月进场费折扣，当月房租折扣,在营进场费折扣，在营房租折扣,空置档口数，预定档口数，招商档口数,已签合同]
    city = {}
    city['全国'] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for row in results:
        city[row[0]] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # 项目数，档口数
    for row in results:
        city['全国'][0] += 1
        city['全国'][1] += row[6]
        city[row[0]][0] += 1
        city[row[0]][1] += row[6]

    sql_con = "select " \
              "case p.area_id " \
              "when 12 then '北京' " \
              "when 14 then '上海'" \
              " when 17 then '深圳' " \
              "when 21 then '杭州' " \
              "else p.area_id end as '项目地区'," \
              "  p.project_name , c.contract_time as '合同签约日', " \
              "case c.is_renewal when 0 then '新签' when 1 then '续签' end as '是否续签'," \
              " c.id as '合同ID',COUNT(*) project_count " \
              "from  project p left join contract c on c.project_id = p.project_id and c.is_delete=0 " \
              "and c.is_valid = 1 " \
              "where p.project_id is not null and c.is_delete=0 " \
              "and p.business_status = 0  " \
              "and DATE_FORMAT( c.contract_time, '%Y%m' ) = DATE_FORMAT( CURDATE( ) ,'%Y%m')  and p.area_id !=17" \
              " GROUP BY project_name"
    cur.execute(sql_con)
    results_con = cur.fetchall()
    for row in results_con:
        city['全国'][5] += 1
        city[row[0]][5] += 1
        if row[3] == '新签':
            city[row[0]][2] += row[5]
            city['全国'][2] += row[5]

        elif row[3] == '续签':
            city[row[0]][3] += row[5]
            city['全国'][3] += row[5]
    sql_out = "select " \
              "case p.area_id " \
              "when 12 then '北京' when 14 then '上海' when 17 then '深圳' when 21 then '杭州' " \
              "else p.area_id end as '项目地区',  " \
              "p.project_name as '项目名称', c.contract_time as '合同签约日', " \
              "case c.is_renewal when 0 then '新签' when 1 then '续签' end as '是否续签'," \
              " c.id as '合同ID', c.end_time as '合同结束时间', c.leave_time as '提前离场时间' " \
              "from  project p left join contract c on c.project_id = p.project_id and c.is_delete=0 and c.is_valid = 0 " \
              "where p.project_id is not null and c.is_delete=0 and c.status in (201,202) and p.business_status = 0 " \
              "and DATE_FORMAT( c.leave_time, '%Y%m' ) = DATE_FORMAT( CURDATE( ) ,'%Y%m') and p.area_id !=17 "
    cur.execute(sql_out)
    results_out = cur.fetchall()
    for row in results_out:
        city[row[0]][4] += 1
        city['全国'][4] += 1

    # 入住档口
    sql_check_in = "select " \
                   "case p.area_id  when 12 then '北京'  " \
                   "when 14 then '上海'  when 17 then '深圳'  when 21 then '杭州'  else p.area_id end as '项目地区'," \
                   "count(*) " \
                   "from  project p left join contract c on c.project_id = p.project_id  " \
                   "where p.project_id is not null and c.is_valid =1  and p.business_status = 0 " \
                   "and DATE_FORMAT( c.start_time, '%Y%m' ) = DATE_FORMAT( CURDATE( ) ,'%Y%m')  " \
                   "and c.start_time < now() and p.area_id !=17  group by p.area_id "
    cur.execute(sql_check_in)
    results_check_in = cur.fetchall()
    for row in results_check_in:
        city[row[0]][6] = row[1]
        city['全国'][6] += row[1]

    # 付款档口
    sql_pay = "select " \
              "case p.area_id  when 12 then '北京'  when 14 then '上海'  when 17 then '深圳'  when 21 then '杭州'  " \
              "else p.area_id end as '项目地区', c.start_time,COUNT(*) " \
              "from  project p left join contract c on c.project_id = p.project_id  " \
              "where p.project_id is not null and c.is_new_contract=1 and p.business_status = 0 " \
              "and DATE_FORMAT( c.start_time, '%Y%m' ) = DATE_FORMAT( CURDATE( ) ,'%Y%m')  and p.area_id !=17" \
              " group by p.area_id"
    cur.execute(sql_pay)
    results_pay = cur.fetchall()
    for row in results_pay:
        city[row[0]][7] = row[2]
        city['全国'][7] += row[2]

    # 档口入住率
    sql_stall_check_in = "SELECT " \
                         "case p.area_id  when 12 then '北京'  when 14 then '上海'  " \
                         "when 17 then '深圳'  when 21 then '杭州' else p.area_id end as '项目地区' ," \
                         "convert(count(c.id)/COUNT(a.stalls_id),decimal(10,4)),  COUNT(a.stalls_id) " \
                         "FROM  stalls a LEFT JOIN contract c " \
                         "on c.stall_id=a.stalls_id and (c.is_valid=1 or c.is_new_contract=1) " \
                         "LEFT JOIN project p on p.project_id = a.project_id where p.is_delete =0 and p.business_status =0 " \
                         "and p.status =2 and a.business_status=0 and p.area_id !=17 GROUP BY p.area_id"
    cur.execute(sql_stall_check_in)
    results_stall_check_in = cur.fetchall()
    city_count = 0
    for row in results_stall_check_in:
        city[row[0]][8] = row[1]
        city['全国'][8] += row[1]
        city_count += 1
    city['全国'][8] = round(city['全国'][8] / city_count, 4)

    # 档口招商率
    sql2 = "SELECT " \
           "case p.area_id  when 12 then '北京'  when 14 then '上海'  when 17 then '深圳'  when 21 then '杭州' " \
           "else p.area_id end as '项目地区' ," \
           "convert((COUNT(sl.id)+count(c.id))/COUNT(a.stalls_id),decimal(10,4))," \
           "COUNT(a.stalls_id),  COUNT(sl.id)+count(c.id) FROM  stalls a " \
           "LEFT JOIN stall_lock sl ON a.stalls_id = sl.stall_id and sl.is_valid=1  " \
           "LEFT JOIN contract c on c.stall_id=a.stalls_id and (c.is_valid=1 or c.is_new_contract=1) " \
           "LEFT JOIN project p on p.project_id = a.project_id where p.is_delete =0 and p.business_status =0  " \
           "and p.status =2 and a.business_status=0 and p.area_id !=17 GROUP BY p.area_id "
    cur.execute(sql2)
    results2 = cur.fetchall()
    city_count2 = 0
    for row in results2:
        city[row[0]][9] = row[1]
        city['全国'][9] += row[1]
        city_count2 += 1
    city['全国'][9] = round(city['全国'][9] / city_count2, 4)

    # 当月签约折扣率
    sql_month = "select case p.area_id  when 12 then '北京'  when 14 then '上海'  when 17 then '深圳'  " \
                "when 21 then '杭州'  else p.area_id end as '项目地区', c.start_time,COUNT(*), " \
                "convert((100-if(c.is_renewal =1,100,avg(c.slotting_discount)))/100,decimal(10,4)) " \
                "as '进场费折扣（续约情况为0）',  convert((100-avg(c.rent_discount))/100,decimal(10,4)) as '房租折扣' " \
                "from  project p left join contract c on c.project_id = p.project_id  where p.project_id is not null " \
                "and (c.is_new_contract=1 or c.is_valid=1) and p.business_status = 0 and DATE_FORMAT( c.start_time, '%Y%m' )" \
                " = DATE_FORMAT( CURDATE( ) ,'%Y%m')  and p.area_id !=17 group by p.area_id"
    cur.execute(sql_month)
    results_month = cur.fetchall()
    city_count3 = 0
    for row in results_month:
        city[row[0]][10] = row[3]
        city['全国'][10] += row[3]
        city[row[0]][11] = row[4]
        city['全国'][11] += row[4]
        city_count3 += 1
    city['全国'][10] = round(city['全国'][10] / city_count3, 4)
    city['全国'][11] = round(city['全国'][11] / city_count3, 4)

    # 在营签约折扣率
    sql_all = "select case p.area_id  when 12 then '北京'  when 14 then '上海'  when 17 then '深圳'  " \
              "when 21 then '杭州'  else p.area_id end as '项目地区', c.start_time,COUNT(*), " \
              "convert((100-if(c.is_renewal =1,100,avg(c.slotting_discount)))/100,decimal(10,4)) " \
              "as '进场费折扣（续约情况为0）',  convert((100-avg(c.rent_discount))/100,decimal(10,4)) as '房租折扣' " \
              "from  project p left join contract c on c.project_id = p.project_id  where p.project_id is not null " \
              "and (c.is_new_contract=1 or c.is_valid=1) and p.business_status = 0 and p.area_id !=17  group by p.area_id"
    cur.execute(sql_all)
    results_all = cur.fetchall()
    city_count4 = 0
    for row in results_all:
        city[row[0]][12] = row[3]
        city['全国'][12] += row[3]
        city[row[0]][13] = row[4]
        city['全国'][13] += row[4]
        city_count4 += 1
    city['全国'][12] = round(city['全国'][12] / city_count4, 4)
    city['全国'][13] = round(city['全国'][13] / city_count4, 4)
    # 空置
    sql_stall = "SELECT p.area_name,s.`status`,COUNT(*),s.business_status from stalls s " \
                "LEFT JOIN project p on s.project_id =p.project_id where p.is_delete=0 " \
                "and s.is_delete=0 " \
                "and p.area_id !=17 GROUP BY p.area_id,s.`status` "
    cur.execute(sql_stall)
    results_stall = cur.fetchall()
    for row in results_stall:
        if row[1] == 0:
            city[row[0]][14] = row[2]
            city['全国'][14] += row[2]
        elif row[1] == 1:
            city[row[0]][15] = row[2]
            city['全国'][15] += row[2]
        elif row[1] == 6:
            city[row[0]][16] = row[2]
            city['全国'][16] += row[2]
        if row[1] == 3:
            city[row[0]][17] = row[2]
            city['全国'][17] += row[2]

    inster_sql = "insert into xmxc_business_data_history  " \
                 "(city_id,city_name,project_count,new_contract_count,continue_contract_count," \
                 "out_contract_count,have_contract_project_count,stalls_count," \
                 "check_in_stalls_count,pay_stalls_count,stalls_check_in_rate," \
                 "attract_investment_rate,check_in_discount_rate,month_contract_rent_discount_rate," \
                 "month_contract_enter_discount_rate,manager_rent_discount_rate,manager_enter_discount_rate," \
                 "empty_stalls_count,reserve_stalls_count,attract_stalls_count,has_contract_count) values" \
                 "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    update_sql = "UPDATE xmxc_business_data set project_count=%s,new_contract_count=%s,continue_contract_count=%s, " \
                 "out_contract_count=%s,have_contract_project_count=%s,stalls_count=%s, " \
                 "check_in_stalls_count=%s,pay_stalls_count=%s,stalls_check_in_rate=%s, " \
                 "attract_investment_rate=%s,check_in_discount_rate=%s,month_contract_rent_discount_rate=%s, " \
                 "month_contract_enter_discount_rate=%s,manager_rent_discount_rate=%s," \
                 "manager_enter_discount_rate=%s,update_time =%s,empty_stalls_count=%s," \
                 "reserve_stalls_count=%s,attract_stalls_count=%s ,has_contract_count=%s where city_id=%s"

    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for row in city.keys():
        values = city.get(row)
        city_id = 0
        if row == '北京':
            city_id = 12
        elif row == '上海':
            city_id = 14
        elif row == '杭州':
            city_id = 21
        elif row == '深圳':
            city_id = 17
        elif row == '全国':
            city_id = 10
        # [项目数，档口数,新签数，续签数,退场,动销数,入住档口,付款档口,档口入住率,档口招商率,当月进场费折扣，当月房租折扣,在营进场费折扣，在营房租折扣]
        insert_data = (
            city_id, row, values[0], values[2], values[3], values[4], values[5], values[1], values[6], values[7],
            values[8], values[9], values[8] * values[12], values[10], values[11], values[12], values[13]
            , values[14], values[15], values[16], values[17])
        cur2.execute(inster_sql, insert_data)
        # update_data = (values[0], values[2], values[3], values[4], values[5], values[1], values[6], values[7],
        #                values[8], values[9], values[8] * values[12], values[10], values[11], values[12], values[13],
        #                nowTime, values[14], values[15], values[16],values[17], city_id)
        # cur2.execute(update_sql, update_data)
    db2.commit()
    print(city)
    db.close()
    db2.close()


# 插入每天入住档口数据
def insert_stall(data):
    db = pool_project.connection()
    cur = db.cursor()
    db2 = pool.connection()
    cur2 = db2.cursor()
    sql = "select " \
          "case p.area_id " \
          "when 12 then '北京' " \
          "when 14 then '上海' " \
          "when 17 then '深圳' " \
          "when 21 then '杭州' " \
          "else p.area_id end city, " \
          "p.project_name , s.stalls_name , s.entry_fee ,s.monthly_rent  , p.business_status  ,COUNT(*) stall_count " \
          "from stalls s left join project p " \
          "on s.project_id = p.project_id and s.is_delete = 0 and p.is_delete = 0 " \
          "where p.project_id is not null " \
          "and p.business_status=0 and p.area_id !=17 " \
          "GROUP BY p.area_id,p.project_name order by p.area_id,p.project_id"
    cur.execute(sql)
    results = cur.fetchall()

    # [项目数，档口数,入住档口,空置档口]
    city = {}

    city['全国'] = [0, 0, 0, 0, data, '全国', ]
    for row in results:
        city[row[0]] = [0, 0, 0, 0, data, row[0], ]
    # 项目数，档口数
    for row in results:
        city['全国'][0] += 1
        city['全国'][1] += row[6]
        city[row[0]][0] += 1
        city[row[0]][1] += row[6]

    # 入住档口
    # sql_check_in = "select " \
    #                "case p.area_id  when 12 then '北京'  " \
    #                "when 14 then '上海'  when 17 then '深圳'  when 21 then '杭州'  else p.area_id end as '项目地区'," \
    #                "count(*),SUBSTRING_INDEX(c.start_time,' ',1) " \
    #                "from  project p left join contract c on c.project_id = p.project_id  " \
    #                "where p.project_id is not null and c.is_valid =1  and p.business_status = 0 " \
    #                "and DATE_FORMAT( c.start_time, '%Y%m' ) = DATE_FORMAT( CURDATE( ) ,'%Y%m')  " \
    #                "and c.start_time < now() and p.area_id !=17  group by p.area_id ,SUBSTRING_INDEX(c.start_time,' ',1) "
    # cur.execute(sql_check_in)
    # results_check_in = cur.fetchall()
    # for row in results_check_in:
    #     data = str(row[2]).split(' ')[0]
    #     city[row[0] + data][2] = row[1]
    #     city['全国' + data][2] += row[1]

    # 空置档口
    sql_empty = "SELECT case city_id when 12 then '北京'  when 14 then '上海'  when 17 then '深圳'  when 21 then '杭州'  " \
                "else city_id end city ,SUM(businessinvitationstalltotalsize-openstallsize-noopenstallsize)," \
                "SUBSTRING_INDEX(create_time,' ',1),openstallsize+noopenstallsize  from statistics.projectandstallinfo where SUBSTRING_INDEX(create_time,' ',1)  " \
                "='%s'  and city_id !=17 GROUP BY city_id,create_time " % data
    cur.execute(sql_empty)
    results_empty = cur.fetchall()

    for row in results_empty:
        city[row[0]][2] = row[3]
        city['全国'][2] += row[3]
        city[row[0]][3] = row[1]
        city['全国'][3] += row[1]

    inster_sql = "insert into xmxc_business_stall_month  " \
                 "(city_id,city_name,check_in_stalls_count,empty_stalls_count,update_time," \
                 "create_time) values" \
                 "(%s,%s,%s,%s,%s,%s)"

    for row in city.keys():
        values = city.get(row)
        city_id = 0
        if values[5] == '北京':
            city_id = 12
        elif values[5] == '上海':
            city_id = 14
        elif values[5] == '杭州':
            city_id = 21
        elif values[5] == '深圳':
            city_id = 17
        elif values[5] == '全国':
            city_id = 10
        # [项目数，档口数,新签数，续签数,退场,动销数,入住档口,付款档口,档口入住率,档口招商率,当月进场费折扣，当月房租折扣,在营进场费折扣，在营房租折扣]
        insert_data = (city_id, values[5], values[2], values[3], values[4], values[4])
        cur2.execute(inster_sql, insert_data)
    #     update_data = (values[0], values[2], values[3], values[4], values[5], values[1], values[6], values[7],
    #                    values[8], values[9], values[8] * values[12], values[10], values[11], values[12], values[13],
    #                    nowTime, city_id)
    #     cur2.execute(update_sql, update_data)
    db2.commit()
    db.close()
    db2.close()


# 插入每天带看数据
def insert_take_out(yesterday):
    db = pool_project.connection()
    cur = db.cursor()
    db2 = pool.connection()
    cur2 = db2.cursor()
    # 获取部门
    sql_department = "select * from `xmxc-user`.xmxc_department where is_deleted = 0  and department_name not like '深圳%' and department_name not in ('弱电', '电销组', '合规监察部', '测试部','技术部','产品部','客服部','财务部','GR','北京维修','上海维修','北京设计组','上海设计组','管理支持部','法务部') and department_id not in (select distinct ifnull(parent_id,'0') from `xmxc-user`.xmxc_department where is_deleted = 0) order by create_time asc"
    cur.execute(sql_department)
    results = cur.fetchall()
    department_dict = {}
    course_dict = {}
    # [部门id，部门名称，parentid，在职人数,离职人数,新签，续签,进场费折扣（续约情况为0）,房租折扣]
    for row in results:
        course_dict[row[0]] = [row[0], row[1], row[2], 0, 0, 0, 0, 0]

    # 部门当前在职人数
    sql_current_person = "select department,count(1) from `xmxc-user`.xmxc_user where  is_deleted = 0 and status = 1 GROUP BY department"
    cur.execute(sql_current_person)
    results_current_person = cur.fetchall()
    for row in results_current_person:
        if row[0] in department_dict.keys():
            course_dict[row[0]][3] = row[1]
    # 创建商户数
    sql_create = "select xu.department,count(1) from merchant m left join `xmxc-user`.xmxc_user xu on m.create_user_id = xu.id where m.is_delete = 0 and xu.is_deleted = 0 AND SUBSTRING_INDEX(m.create_time,' ',1)  = '%s' GROUP BY xu.department " % yesterday

    cur.execute(sql_create)
    results_create = cur.fetchall()

    for row in results_create:
        if row[0] in course_dict.keys():
            course_dict[row[0]][5] = row[1]

    # 某个部门当月带看人数
    sql_take_look = "select xu.department, count(1) from look_record lr left join `xmxc-user`.xmxc_user xu on lr.user_id = xu.id where lr.is_delete = 0 and xu.is_deleted = 0 AND SUBSTRING_INDEX(lr.look_time,' ',1)  = '%s'  GROUP BY xu.department " % yesterday
    cur.execute(sql_take_look)
    results_take_look = cur.fetchall()
    for row in results_take_look:
        if row[0] in course_dict.keys():
            course_dict[row[0]][6] = row[1]

    # 某个部门当月签约人数
    sql_con_count = "select xu.department,count(1) from contract ct left join `xmxc-user`.xmxc_user xu on ct.business_user_id = xu.id where ct.is_delete = 0 and xu.is_deleted = 0 AND SUBSTRING_INDEX(ct.contract_time,' ',1)  = '%s'  GROUP BY xu.department" % yesterday
    cur.execute(sql_con_count)
    results_con_count = cur.fetchall()
    for row in results_con_count:
        if row[0] in course_dict.keys():
            course_dict[row[0]][7] = row[1]

    # 部门当月离职人数
    sql_out_person = "select department,count(1) from `xmxc-user`.xmxc_user where   is_deleted = 0 and status = 0  and DATE_FORMAT( update_time, '%Y%m' ) = DATE_FORMAT( CURDATE( ) ,'%Y%m')  GROUP BY department"
    cur.execute(sql_out_person)
    results_out_person = cur.fetchall()
    for row in results_out_person:
        if row[0] in department_dict.keys():
            course_dict[row[0]][4] = row[1]
    insert_sql2 = "insert into business_contract_course_info " \
                  "(department_id,department_name,begin_month_person_count,current_person_count,creating_mark_count" \
                  ",take_look_complete_count,contract_count,create_time) values " \
                  "(%s,%s,%s,%s,%s,%s,%s,%s)"
    for row in course_dict.keys():
        values = course_dict.get(row)
        insert_data = (
            values[0], values[1], values[3] + values[4], values[3], values[5], values[6], values[7], yesterday)
        cur2.execute(insert_sql2, insert_data)
        db2.commit()
    # print(department_dict)
    db.close()
    db2.close()


def main():
    yesterday = date.today() + timedelta(days=-1)
    insert_data()
    # insert_stall(str(yesterday))
    # insert_take_out(yesterday=yesterday)


if __name__ == "__main__":
    main()
