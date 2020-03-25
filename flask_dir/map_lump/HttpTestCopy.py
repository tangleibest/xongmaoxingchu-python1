# coding:utf-8
from typing import Any, Union, Tuple
from flask import Flask, request
import pymysql  # 导入 pymysql
import json
import sys
from flask_cors import CORS

reload(sys)

sys.setdefaultencoding('utf8')
app = Flask(__name__)
CORS(app, supports_credentials=True)

s = 0.1
a = 0.3
b = 0.6
c = 0.4


# 地图块详细数据
@app.route('/getLumpData')
def hello_world():
    # 打开数据库连接
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="test", port=62864)

    # 使用cursor()方法获取操作游标
    cur = db.cursor()
    level = request.args.get('level')
    sql_all = "SELECT * from (SELECT lump_id,COUNT(1) mark_count,SUM(month_sale_num) sale from t_map_client_elm_beijing_mark where own_set_cate='快餐便当' and lump_id is not null GROUP BY lump_id) ta_1 LEFT JOIN t_map_lat_lng ta_2 on ta_1.lump_id=ta_2.lump_id ORDER BY sale desc"
    cur.execute(sql_all)  # 执行sql语句
    results_all = cur.fetchall()
    results_all = list(results_all)

    allsale = 0
    list_len = 0
    for row1 in results_all:
        allsale += int(row1[2])
        list_len += 1
    sq = []
    if level == 'S':
        list_shop = results_all[0: int(list_len * s) + 1]
        for row in list_shop:
            data = {}
            data['id'] = row[0]
            data['left_lower'] = row[4]
            data['left_up'] = row[5]
            data['right_up'] = row[7]
            data['right_lower'] = row[6]
            data['mark_num'] = str(row[1])
            data['month_sale'] = str(row[2])
            data['all_mark_num'] = str(allsale)
            data['all_month_sale'] = str(list_len)

            sq.append(data)
    elif level == 'A':
        list_shop = results_all[int(list_len * s):int(list_len * a) + 1]
        for row in list_shop:
            data = {}
            data['id'] = row[0]
            data['left_lower'] = row[4]
            data['left_up'] = row[5]
            data['right_up'] = row[7]
            data['right_lower'] = row[6]
            data['mark_num'] = str(row[1])
            data['month_sale'] = str(row[2])
            data['all_mark_num'] = str(allsale)
            data['all_month_sale'] = str(list_len)

            sq.append(data)
    elif level == 'B':
        list_shop = results_all[int(list_len * a): int(list_len * b) + 1]
        for row in list_shop:
            data = {}
            data['id'] = row[0]
            data['left_lower'] = row[4]
            data['left_up'] = row[5]
            data['right_up'] = row[7]
            data['right_lower'] = row[6]
            data['mark_num'] = str(row[1])
            data['month_sale'] = str(row[2])
            data['all_mark_num'] = str(allsale)
            data['all_month_sale'] = str(list_len)

            sq.append(data)
    else:
        list_shop = results_all[int(list_len * b): list_len - 1]
        for row in list_shop:
            data = {}
            data['id'] = row[0]
            data['left_lower'] = row[4]
            data['left_up'] = row[5]
            data['right_up'] = row[7]
            data['right_lower'] = row[6]
            data['mark_num'] = str(row[1])
            data['month_sale'] = str(row[2])
            data['all_mark_num'] = str(allsale)
            data['all_month_sale'] = str(list_len)

            sq.append(data)

    jsondatar = json.dumps(sq, ensure_ascii=False)

    db.close()  # 关闭连接
    return jsondatar


# 店铺分级标签
@app.route('/getLevel')
def get_Level():
    s1 = int(s * 100)
    a1 = int(a * 100)
    b1 = int(b * 100)
    c1 = int(c * 100)
    level = []
    detaLevel = {}
    detaLevel['S'] = "单量占比前百分之%s" % s1
    detaLevel['A'] = "单量占比百分之%s~百分之%s" % (s1, a1)
    detaLevel['B'] = "单量占比百分之%s~百分之%s" % (a1, b1)
    detaLevel['C'] = "单量占比后百分之%s" % c1
    level.append(detaLevel)
    jsonLevel = json.dumps(level, ensure_ascii=False)
    return jsonLevel


# 熊猫门店数据
@app.route('/getXmMark')
def get_xm_mark():
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="redsquirrel", port=62864)
    cur = db.cursor()
    sql = "SELECT mark_name,address,longitude,latitude from xmxc_shop_info where city_name='北京'"
    cur.execute(sql)
    results = cur.fetchall()
    sq = []
    for row in results:
        data = {}
        data['mark_name'] = row[0]
        data['address'] = row[1]
        data['longitude'] = row[2]
        data['latitude'] = row[3]
        sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    # print(id)
    db.close()
    return jsondatar


# 熊猫门店数据
@app.route('/getXmMarkData')
def get_xm_mark_data():
    mark = request.args.get('mark')
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="redsquirrel", port=62864)
    cur = db.cursor()

    sql = "SELECT CLIENT_NAME,PROJECT_NAME,SUM(STREAM) sum_money FROM t_shop_info_archive a LEFT JOIN xmxc_shop_info b ON a.PROJECT_NAME = b.mark_name WHERE b.city_name = '北京' AND a.STREAM_DATE BETWEEN (	DATE_SUB(DATE_FORMAT(NOW(), '%Y-%m-%d'),INTERVAL 31 DAY ))AND (	DATE_SUB(	DATE_FORMAT(NOW(), '%Y-%m-%d'),	INTERVAL 1 DAY )) AND PROJECT_NAME='" + mark + "' GROUP BY CLIENT_NAME ORDER BY sum_money DESC"
    cur.execute(sql)
    results = cur.fetchall()
    sq = []
    for row in results:
        data = {}
        data['CLIENT_NAME'] = row[0]
        data['PROJECT_NAME'] = row[1]
        data['sum_money'] = str(row[2])
        sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)

    db.close()
    return jsondatar


# 获取块内详细外卖店铺
@app.route('/getLumpMark')
def get_lump_mark():
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="test", port=62864)

    cur = db.cursor()
    lumpId = request.args.get('lumpId')
    sql = "SELECT lump_id,client_name,month_sale_num,longitude,latitude from  t_map_client_elm_beijing_mark where lump_id = " + lumpId + " and first_cate not like '%医药健康%' and first_cate not like '%超市%' and first_cate not like '%果蔬生鲜%'"
    cur.execute(sql)
    results = cur.fetchall()
    sq = []
    for row in results:
        data = {}
        data['lump_id'] = str(row[0])
        data['client_name'] = row[1]
        data['month_sale_num'] = str(row[2])
        data['longitude'] = row[3]
        data['latitude'] = row[4]
        sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    # print(id)
    db.close()
    return jsondatar


if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host='0.0.0.0', port=5000, debug=True)
