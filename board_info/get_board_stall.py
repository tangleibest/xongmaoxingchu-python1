import datetime
import time

import pandas as pd

import pymysql
from DBUtils.PooledDB import PooledDB

pool_project = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei',
                        passwd='tanglei', db='commerce',
                        port=3306)

pool = PooledDB(pymysql, 5, host='192.168.31.126', user='root',
                passwd='123456', db='finebi',
                port=3306)


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


def insert_stall():
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
    s_data = getTheMonth(0, '%Y-%m-%d')
    e_data = datetime.datetime.now().strftime('%Y-%m-%d')
    be_data = getDatesByTimes(s_data, e_data)
    for data in be_data:
        city['全国' + data] = [0, 0, 0, 0, data, '全国', ]
        for row in results:
            city[row[0] + data] = [0, 0, 0, 0, data, row[0], ]
    # 项目数，档口数
    for row in results:
        for data in be_data:
            city['全国' + data][0] += 1
            city['全国' + data][1] += row[6]
            city[row[0] + data][0] += 1
            city[row[0] + data][1] += row[6]

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
                "SUBSTRING_INDEX(create_time,' ',1),openstallsize+noopenstallsize  from statistics.projectandstallinfo where create_time " \
                "BETWEEN '%s' and '%s' and city_id !=17 GROUP BY city_id,create_time " % (s_data, e_data)
    cur.execute(sql_empty)
    results_empty = cur.fetchall()
    for row in results_empty:
        city[row[0] + row[2]][2] = row[3]
        city['全国' + row[2]][2] += row[3]
        city[row[0] + row[2]][3] = row[1]
        city['全国' + row[2]][3] += row[1]

    inster_sql = "insert into xmxc_business_stall_month  " \
                 "(city_id,city_name,check_in_stalls_count,empty_stalls_count,update_time," \
                 "create_time) values" \
                 "(%s,%s,%s,%s,%s,%s)"

    update_sql = "UPDATE xmxc_business_data set project_count=%s,new_contract_count=%s,continue_contract_count=%s, " \
                 "out_contract_count=%s,have_contract_project_count=%s,stalls_count=%s, " \
                 "check_in_stalls_count=%s,pay_stalls_count=%s,stalls_check_in_rate=%s, " \
                 "attract_investment_rate=%s,check_in_discount_rate=%s,month_contract_rent_discount_rate=%s, " \
                 "month_contract_enter_discount_rate=%s,manager_rent_discount_rate=%s," \
                 "manager_enter_discount_rate=%s,update_time =%s where city_id=%s"

    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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


def main():
    # while True:
    insert_stall()
    # time.sleep(300)


if __name__ == "__main__":
    main()
