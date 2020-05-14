import csv

import pymysql
from DBUtils.PooledDB import PooledDB
from datetime import date, timedelta
import time

twepoch = 1288834974657
datacenter_id_bits = 1
worker_id_bits = 1
sequence_id_bits = 9
max_datacenter_id = 1 << datacenter_id_bits
max_worker_id = 1 << worker_id_bits
max_sequence_id = 1 << sequence_id_bits
max_timestamp = 1 << (64 - datacenter_id_bits - worker_id_bits - sequence_id_bits)


# 雪花算法计算id
def make_snowflake(timestamp_ms, datacenter_id, worker_id, sequence_id, twepoch=twepoch):
    sid = ((int(timestamp_ms) - twepoch) % max_timestamp) << datacenter_id_bits << worker_id_bits << sequence_id_bits
    sid += (datacenter_id % max_datacenter_id) << worker_id_bits << sequence_id_bits
    sid += (worker_id % max_worker_id) << sequence_id_bits
    sid += sequence_id % max_sequence_id
    return sid


yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
# 数据库连接池
pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                                db='mapmarktest', port=62864)
pool_project = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei',
                        passwd='tanglei', db='commerce', port=3306)
pool_data = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei',
                     passwd='tanglei', db='data_visualization', port=3306)

db = pool_mapmarkeronline.connection()
cur = db.cursor()

db_project = pool_project.connection()
cur_project = db_project.cursor()
# 查询销量表档口商户
db_data = pool_data.connection()
cur_data = db_data.cursor()
sql_project = "SELECT table2.project_id,table2.project_name,table2.stalls_id,table2.stalls_name,table2.merchant_id, public_sea_pool.`name`,table2.area_name from  (SELECT	table1.*, contract.merchant_id FROM (SELECT	b.project_id,b.project_name,a.stalls_id,a.stalls_name,b.area_name FROM stalls a LEFT JOIN project b ON a.project_id = b.project_id ) table1 LEFT JOIN contract ON table1.stalls_id = contract.stall_id WHERE contract.is_valid = 1 and contract.is_delete=0) table2 LEFT JOIN  public_sea_pool on table2.merchant_id=public_sea_pool.id ORDER BY  table2.project_name desc "

cur_project.execute(sql_project)
results_project = cur_project.fetchall()
# 查询外卖邦商户
sql = "SELECT * from ( SELECT sort_name,shop_name,shop_id,substring_index(substring_index(c.client_code,'-' ,d.help_topic_id+1),'-',-1)" \
      " client_code,city_name from (SELECT a.sort_name,a.shop_name,a.shop_id,a.client_code,a.city_name from t_map_client_wmb_shop a" \
      "  RIGHT JOIN  (SELECT MAX(shop_id) shop_id from t_map_client_wmb_shop where sort_name is not null and client_code is not " \
      "null GROUP BY sort_name,client_code) b on a.shop_id=b.shop_id) c join mysql.help_topic d on d.help_topic_id < " \
      "(length(c.client_code) - length(replace(c.client_code,'-',''))+1)  ) m "

cur.execute(sql)
results = cur.fetchall()

dict_info = {}

# 联合销控表和外卖邦商户，构造商户列表
for row_project in results_project:
    project_id = row_project[0]
    project_name = row_project[1]
    if project_name == "望京":
        project_name = "望京商业中心"

    stall_id = row_project[2]
    stall_name = str(row_project[3]).replace('+', '').strip('(分割)')
    merchant_id = row_project[4]
    merchant_name = row_project[5]

    project_name2 = ''

    wmb_project = ''
    wmb_shop = ''
    wmb_stall = ''
    wmb_id = 0

    if '店' in project_name:
        project_name2 = str(project_name).strip('1店').strip('一店').strip('店')
    else:
        project_name2 = project_name

    if '二店' not in project_name:
        for row_wmb in results:
            if '二店' not in str(row_wmb[0]):
                if project_name2 in str(row_wmb[0]):
                    # print(project_name, stall_id, row_wmb[0], row_wmb[3])
                    if str(stall_name) == str(row_wmb[3]).strip('档口').replace('+', ''):
                        wmb_project = row_wmb[0]
                        wmb_shop = row_wmb[1]
                        wmb_stall = row_wmb[3]
                        wmb_id = row_wmb[2]
    else:
        for row_wmb in results:
            if '二店' in str(row_wmb[0]):
                if project_name2 in str(row_wmb[0]):
                    # print(project_name, stall_id, row_wmb[0], row_wmb[3])
                    if str(stall_name) == str(row_wmb[3]).strip('档口').replace('+', ''):
                        wmb_project = row_wmb[0]
                        wmb_shop = row_wmb[1]
                        wmb_stall = row_wmb[3]
                        wmb_id = row_wmb[2]
    if project_name == "望京商业中心":
        project_name = "望京"
    dict_info[str(wmb_id) + wmb_stall] = [project_id, project_name, merchant_id, merchant_name, stall_id, stall_name,
                                          wmb_project,
                                          wmb_shop,
                                          wmb_stall, wmb_id, row_project[6]]

# 查询外卖邦销售数据
sql_income = "SELECT tableb.shop_id,tableb.income_amount,tableb.count_sale,tableb.source,tableb.date,tablea.client_code,tablea.shop_name from (SELECT * from ( SELECT sort_name,shop_name,shop_id,substring_index(substring_index(c.client_code,'-' ,d.help_topic_id+1),'-',-1) client_code,city_name from (SELECT a.sort_name,a.shop_name,a.shop_id,a.client_code,a.city_name from t_map_client_wmb_shop a RIGHT JOIN  (SELECT MAX(shop_id) shop_id from t_map_client_wmb_shop where sort_name is not null and client_code is not  null GROUP BY sort_name,client_code) b on a.shop_id=b.shop_id) c join mysql.help_topic d on d.help_topic_id <  (length(c.client_code) - length(replace(c.client_code,'-',''))+1)  ) m) tablea LEFT JOIN (SELECT shop_id,SUM(income_amount) income_amount,COUNT(*) count_sale,source,date from t_map_client_wmb_user_2019_2 where date='%s' and send_status !='error' GROUP BY shop_id,source) tableb on tablea.shop_id=tableb.shop_id where tableb.shop_id is not null" % yesterday
cur.execute(sql_income)
results_income = cur.fetchall()
list_data = {}
# 把每个商户构造三个平台数据
for row_list in results_income:
    if str(row_list[0]) + str(row_list[5]) in dict_info.keys():
        time.sleep(0.1)
        snowflake_id = make_snowflake(time.time() * 1000, 1, 0, 0)
        info_list = dict_info.get(str(row_list[0]) + str(row_list[5]))
        project_id2 = info_list[0]
        project_name3 = info_list[1]
        merchant_id2 = info_list[2]
        merchant_name2 = info_list[3]
        stall_id2 = info_list[4]
        stall_name2 = info_list[5]
        date = str(row_list[4]).replace('-', '')
        list_data[str(row_list[0]) + '堂食' + stall_name2] = [snowflake_id, project_id2, project_name3, stall_id2,
                                                            stall_name2,
                                                            merchant_id2, merchant_name2,
                                                            100, 200, 0, 0, 0.0, date, 0, '外卖邦', '外卖邦', '0']

        time.sleep(0.1)
        snowflake_id1 = make_snowflake(time.time() * 1000, 1, 0, 0)
        list_data[str(row_list[0]) + 'meituan' + stall_name2] = [snowflake_id1, project_id2, project_name3, stall_id2,
                                                                 stall_name2,
                                                                 merchant_id2, merchant_name2, 200, 200, 0, 0, 0.0,
                                                                 date, 0, '外卖邦',
                                                                 '外卖邦', '0']

        time.sleep(0.1)
        snowflake_id2 = make_snowflake(time.time() * 1000, 1, 0, 0)

        list_data[str(row_list[0]) + 'elem' + stall_name2] = [snowflake_id2, project_id2, project_name3, stall_id2,
                                                              stall_name2,
                                                              merchant_id2, merchant_name2, 300, 200, 0, 0, 0.0, date,
                                                              0, '外卖邦',
                                                              '外卖邦', '0']
# 把外卖邦商户更新到商户列表中
for row_income in results_income:
    if str(row_income[0]) + str(row_income[5]) in dict_info.keys():
        info_list = dict_info.get(str(row_income[0]) + str(row_income[5]))

        project_id2 = info_list[0]
        project_name3 = info_list[1]
        merchant_id2 = info_list[2]
        merchant_name2 = info_list[3]
        stall_id2 = info_list[4]
        stall_name2 = info_list[5]
        sale_amount = row_income[1]
        order_amount = row_income[2]

        plat_type = 200
        average = round(sale_amount / order_amount, 2)
        date = str(row_income[4]).replace('-', '')
        revision = 0
        create_by = '外卖邦'
        update_by = '外卖邦'
        is_delete = 0

        if row_income[3] == 'elem':
            insert_list = list_data.get(str(row_income[0]) + 'elem' + stall_name2)

            insert_list[9] = order_amount
            insert_list[10] = sale_amount
            insert_list[11] = average

        elif row_income[3] == 'meituan':
            insert_list = list_data.get(str(row_income[0]) + 'meituan' + stall_name2)

            insert_list[9] = order_amount
            insert_list[10] = sale_amount
            insert_list[11] = average

        elif row_income[3] == '堂食':
            insert_list = list_data.get(str(row_income[0]) + '堂食' + stall_name2)

            insert_list[9] = order_amount
            insert_list[10] = sale_amount
            insert_list[11] = average
        # print(row_income[0],info_list,insert_list[9],insert_list[10])
# 插入商户
for insert_row in list_data.values():
    list2 = insert_row
    insert_sql = "INSERT into  merchant_sales_statistics (tid,project_id,project_name,stall_id,stall_name,merchant_id,merchant_name,channel_type,plat_type," \
                 "order_count,sale_amount,average,stream_date,revision,create_by,update_by,is_delete) values (%s,%s,'%s',%s,'%s',%s,'%s',%s,'%s',%s,%s,%s,'%s',%s,'%s','%s',%s)" \
                 % (list2[0], list2[1], list2[2], list2[3], list2[4], list2[5], list2[6], list2[7], list2[8], list2[9],
                    list2[10], list2[11], list2[12], list2[13], list2[14], list2[15], list2[16])

    cur.execute(insert_sql)  # 执行sql语句
    db.commit()  # 提交到数据库执行

db.close()
db_project.close()
db_data.close()
print("%s更新成功" % yesterday)
