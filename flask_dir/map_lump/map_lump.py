#coding:utf-8
from flask import Flask, request
import pymysql
import json
from DBUtils.PooledDB import PooledDB
import sys
from flask_cors import CORS
reload(sys)
sys.setdefaultencoding('utf8')
app = Flask(__name__)
CORS(app, supports_credentials=True)
pool = PooledDB(pymysql,5,host='bj-cdb-cwu7v42u.sql.tencentcdb.com',user='root',passwd='xmxc1234',db='test',port=62864) #5为连接池里的最少连接数
#地图块详细数据
@app.route('/getLumpData')
def get_lump_data():
    # 打开数据库连接
    # db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
    #                      password="xmxc1234", db="test", port=62864)
    db = pool.connection()
    # 使用cursor()方法获取操作游标
    cur = db.cursor()
    level=request.args.get('level')

    sql_all="SELECT * from (SELECT lump_id,COUNT(1) mark_count,SUM(month_sale_num) sale from t_map_client_elm_beijing_mark where own_set_cate='快餐便当' and lump_id is not null GROUP BY lump_id) ta_1 LEFT JOIN t_map_lat_lng ta_2 on ta_1.lump_id=ta_2.lump_id ORDER BY sale desc"
    cur.execute(sql_all)  # 执行sql语句
    results_all = cur.fetchall()
    allsale=0
    list_len=0
    for row in results_all:
        allsale+=row[2]
        list_len+=1
    sq = []
    if level=='S':
        list_shop=results_all[0:int(list_len*0.3)+1]
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
            data['mark_proportion'] = str(row[9])
            data['month_sale_proportion'] = str(row[10])
            sq.append(data)
    elif level=='A':
        list_shop = results_all[int(list_len * 0.3):int(list_len * 0.6) + 1]
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
            data['mark_proportion'] = str(row[9])
            data['month_sale_proportion'] = str(row[10])
            sq.append(data)
    elif level=='B':
        list_shop = results_all[int(list_len * 0.6):int(list_len * 0.8) + 1]
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
            data['mark_proportion'] = str(row[9])
            data['month_sale_proportion'] = str(row[10])
            sq.append(data)
    else:
        list_shop = results_all[int(list_len * 0.8):list_len-1]
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
            data['mark_proportion'] = str(row[9])
            data['month_sale_proportion'] = str(row[10])
            sq.append(data)

    jsondatar = json.dumps(sq, ensure_ascii=False)
    # print(id)

    db.close()  # 关闭连接
    return jsondatar

#店铺分级标签
@app.route('/getLevel')
def get_Level():
    level=[]
    detaLevel={}
    detaLevel['S']="店铺数大于等于10"
    detaLevel['A']="店铺数大于等于5且小于10"
    detaLevel['B']="店铺数大于等于3且小于5"
    detaLevel['C']="店铺数小于3"
    level.append(detaLevel)
    jsonLevel=json.dumps(level,ensure_ascii=False)
    return jsonLevel

#熊猫门店数据
@app.route('/getXmMark')
def get_xm_mark():
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="redsquirrel", port=62864)
    cur = db.cursor()
    sql="SELECT mark_name,address,longitude,latitude from xmxc_shop_info where city_name='北京'"
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

#熊猫门店数据
@app.route('/getXmMarkData')
def get_xm_mark_data():
    mark = request.args.get('mark')
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="redsquirrel", port=62864)
    cur = db.cursor()

    sql="SELECT CLIENT_NAME,PROJECT_NAME,SUM(STREAM) sum_money FROM t_shop_info_archive a LEFT JOIN xmxc_shop_info b ON a.PROJECT_NAME = b.mark_name WHERE b.city_name = '北京' AND a.STREAM_DATE BETWEEN (	DATE_SUB(DATE_FORMAT(NOW(), '%Y-%m-%d'),INTERVAL 31 DAY ))AND (	DATE_SUB(	DATE_FORMAT(NOW(), '%Y-%m-%d'),	INTERVAL 1 DAY )) AND PROJECT_NAME='"+mark+"' GROUP BY CLIENT_NAME ORDER BY sum_money DESC"
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

#获取块内详细外卖店铺
@app.route('/getLumpMark')
def get_lump_mark():

    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="test", port=62864)
    cur = db.cursor()
    lumpId = request.args.get('lumpId')
    sql="SELECT lump_id,client_name,month_sale_num,longitude,latitude from  t_map_client_elm_beijing_mark where lump_id = "+lumpId+" and first_cate not like '%医药健康%' and first_cate not like '%超市%' and first_cate not like '%果蔬生鲜%'"
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
    print(id)
    db.close()
    return jsondatar

if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host='192.168.31.126', port=5000)
