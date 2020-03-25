# coding:utf-8
from DBUtils.PooledDB import PooledDB
import pymysql
import csv

"""
获取菜品排名前5个
"""

city_name = ['mt_beijing']
for city_row in city_name:
    # 数据库连接池
    pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root',
                                    passwd='xmxc1234',
                                    db='mapmarkeronline', port=62864)
    pool_test = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                         db='test', port=62864)

    db = pool_mapmarkeronline.connection()
    cur = db.cursor()

    db_test = pool_test.connection()
    cur_test = db_test.cursor()
    if 'elm' in city_row:
        sql = "SELECT shop_id,client_name,area_id,address,shipping_time,first_cate,second_cate,rating,month_sale_num,promotions,call_center from t_map_client_%s_mark where update_count=13" % city_row
    else:
        sql = "SELECT shop_id,client_name,area_id,address,shipping_time,first_cate_name,second_cate_name,rating,month_sale_num,promotions,call_center,average_price from t_map_client_%s_mark where update_count=11" % city_row
    sq = []
    print(sql)
    cur.execute(sql)
    results = cur.fetchall()
    with open('11月%s.csv' % city_row, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for row in results:
            food_sql = "SELECT DISTINCT food_name,month_sale FROM get_food_top5  where   shop_id='%s'  ORDER BY month_sale desc LIMIT 5" % \
                       row[0]
            cur_test.execute(food_sql)
            results_food = cur_test.fetchall()
            food_list = []
            for row_food in results_food:
                food_list.append('%s-%s' % (row_food[0], row_food[1]))
            shop_list = [row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                         row[11]] + food_list
            # print(shop_list)
            writer.writerow(shop_list)

    db.close()
