# coding:utf-8
import datetime
import os

from DBUtils.PooledDB import PooledDB
from flask import Flask, request, Blueprint
import pymysql
import json
from datetime import date, timedelta
import time

# from flask_cors import *
from flask import Response

"""
老商户中心数据进入熊猫管家端数据库
"""
app = Flask(__name__)
# CORS(app, supports_credentials=True)
app.config['API_DOC_MEMBER'] = ['api', 'platform']

api = Blueprint('api', __name__)
platform = Blueprint('platform', __name__)

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


def file_iterator(file_path, chunk_size=512):
    """
        文件读取迭代器
    :param file_path:文件路径
    :param chunk_size: 每次读取流大小
    :return:
    """
    with open(file_path, 'rb') as target_file:
        while True:
            chunk = target_file.read(chunk_size)
            if chunk:
                yield chunk
            else:
                break


def to_json(obj):
    """
        放置
    :return:
    """
    return json.dumps(obj, ensure_ascii=False)


pool = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234', db='redsquirrel',
                port=62864)  # 5为连接池里的最少连接数
pool_commerce = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei',
                         passwd='tanglei', db='commerce', port=3306)
pool_visualization = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com',
                              user='tanglei',
                              passwd='tanglei', db='data_visualization', port=3306)


# 返回门店列表
@app.route('/get_project')
def getProject():
    db = pool_commerce.connection()
    cur = db.cursor()
    sql = "SELECT project_id,project_name,area_id,area_name from project"
    cur.execute(sql)
    results_project = cur.fetchall()
    sq = []
    for row in results_project:
        dict_info = {}
        dict_info['project_id'] = row[0]
        dict_info['project_name'] = row[1]
        dict_info['area_id'] = row[2]
        dict_info['area_name'] = row[3]
        sq.append(dict_info)

    jsondatar = json.dumps(sq, ensure_ascii=False)

    return jsondatar


@app.route('/get_project_data')
def getProjectData():
    db_visualization = pool_visualization.connection()
    cur_visualization = db_visualization.cursor()

    project_id = request.args.get('project_id')

    sql = "SELECT project_id,project_name,stream_date FROM merchant_sales_statistics where project_id=%s GROUP BY stream_date" % project_id
    cur_visualization.execute(sql)
    results = cur_visualization.fetchall()
    project_name = results[0][1]
    day_dict = {}
    for day_indax in range(-30, 1):
        day = str((date.today() + timedelta(days=day_indax)).strftime("%Y%m%d"))
        day_dict[project_name + day] = [project_id, project_name, day, "未录入"]
    print(day_dict)
    for row in results:
        if str(row[1]) + str(row[2]) in day_dict.keys():
            values = day_dict.get(str(row[1]) + str(row[2]))
            values[3] = "已录入"
    list_re = []
    for row_dict in day_dict.values():
        dict_re = {}

        dict_re['project_id'] = row_dict[0]
        dict_re['project_name'] = row_dict[1]
        dict_re['stream_date'] = row_dict[2]
        dict_re['is_input'] = row_dict[3]
        list_re.append(dict_re)
    list_re = sorted(list_re, key=lambda x: x["stream_date"], reverse=True)
    jsondatar = json.dumps(list_re, ensure_ascii=False)

    return jsondatar


@app.route('/get_shop_list')
def getShopList():
    db_visualization = pool_visualization.connection()
    cur_visualization = db_visualization.cursor()

    db_commerce = pool_commerce.connection()
    cur_commerce = db_commerce.cursor()

    project_id = request.args.get('project_id')
    stream_date = request.args.get('stream_date')
    sql_project = "SELECT table2.project_id,table2.project_name,table2.stalls_id,table2.stalls_name,table2.merchant_id, " \
                  "public_sea_pool.`name`,table2.area_name from  (SELECT	table1.*, contract.merchant_id FROM (SELECT	" \
                  "b.project_id,b.project_name,a.stalls_id,a.stalls_name,b.area_name FROM stalls a LEFT JOIN project b ON " \
                  "a.project_id = b.project_id ) table1 LEFT JOIN contract ON table1.stalls_id = contract.stall_id WHERE " \
                  "contract.is_valid = 1 and contract.is_delete=0) table2 LEFT JOIN  public_sea_pool on table2.merchant_id=" \
                  "public_sea_pool.id where table2.project_id='%s'" % project_id

    cur_commerce.execute(sql_project)
    results_project = cur_commerce.fetchall()
    dict_stalls = {}
    for row in results_project:
        dict_stalls[str(row[0]) + str(row[2])] = [row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                                                  0, 0.0, 0.0, 0, 0.0, 0.0, 0, 0.0, 0.0]

    sql_sale = "SELECT project_id,project_name,stall_id,stall_name,merchant_id,merchant_name,plat_type,channel_type,order_count,sale_amount,average from merchant_sales_statistics where stream_date='%s' and project_id=%s" % (
        stream_date, project_id)
    cur_visualization.execute(sql_sale)
    results_visualization = cur_visualization.fetchall()

    for row_vis in results_visualization:
        if str(row_vis[0]) + str(row_vis[2]) in dict_stalls.keys():

            if row_vis[7] == 200:

                stall_list = dict_stalls.get(str(row_vis[0]) + str(row_vis[2]))
                stall_list[7] = int(row_vis[8])
                stall_list[8] = float(row_vis[9])
                stall_list[9] = float(row_vis[10])
            elif row_vis[7] == 300:
                stall_list = dict_stalls.get(str(row_vis[0]) + str(row_vis[2]))
                stall_list[10] = int(row_vis[8])
                stall_list[11] = float(row_vis[9])
                stall_list[12] = float(row_vis[10])
            elif row_vis[7] == 100:
                stall_list = dict_stalls.get(str(row_vis[0]) + str(row_vis[2]))
                stall_list[13] = int(row_vis[8])
                stall_list[14] = float(row_vis[9])
                stall_list[15] = float(row_vis[10])
    list_json = []
    for row in dict_stalls.values():
        dict_json = {}
        dict_json['project_id'] = row[0]
        dict_json['project_name'] = row[1]
        dict_json['stall_id'] = row[2]
        dict_json['merchant_id'] = row[4]
        dict_json['stall_name'] = row[3]
        dict_json['shop_name'] = row[5]
        dict_json['meituan_order_count'] = row[7]
        dict_json['meituan_sale_amount'] = row[8]
        dict_json['meituan_average'] = row[9]
        dict_json['elm_order_count'] = row[10]
        dict_json['elm_sale_amount'] = row[11]
        dict_json['elm_average'] = row[12]
        dict_json['tangshi_order_count'] = row[13]
        dict_json['tangshi_sale_amount'] = row[14]
        dict_json['tangshi_average'] = row[15]
        dict_json['stream_date'] = stream_date
        list_json.append(dict_json)

    jsondatar = json.dumps(list_json, ensure_ascii=False)
    return jsondatar


@app.route('/insert_data', methods=['POST'])
def insertData():
    postdata = request.form
    snowflake_id = make_snowflake(time.time() * 1000, 1, 0, 0)

    project_id = postdata['project_id']

    return "chenggong"


app.register_blueprint(api, url_prefix='/api')
app.register_blueprint(platform, url_prefix='/platform')

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
