# coding:utf-8
import math

from DBUtils.PooledDB import PooledDB
from flask import Flask, request,Blueprint
# from flask_docs import ApiDoc
import pymysql
# from flask_cors import *
import json
import time
import datetime
import pandas as pd
import redis

"""
接口连接redis测试
"""
app = Flask(__name__)


EARTH_REDIUS = 6378.137


# 获取日期
def getdate(beforeOfDay, strf):
    today = datetime.datetime.now()
    # 计算偏移量
    offset = datetime.timedelta(days=-beforeOfDay)
    # 获取想要的日期的时间
    re_date = (today + offset).strftime(strf)
    return re_date


# 计算两个经纬度之间的距离
def rad(d):
    return d * math.pi / 180.0


def getDistance(lat1, lng1, lat2, lng2):
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)
    s = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(
        math.sin(b / 2), 2)))
    s = s * EARTH_REDIUS
    return s

#redis连接池
pool = redis.ConnectionPool(host='139.199.112.205', port=6379,password='xmxc1234',db=1,decode_responses=True)
redis_conn = redis.Redis(connection_pool=pool)

#数据库连接池
pool_mapmarkeronline = PooledDB(pymysql,mincached= 100,maxcached=500, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                                db='mapmarkeronline', port=62864)
#pool_project = PooledDB(pymysql,5, host='140.143.78.200', user='root', passwd='xmxc1234', db='commerce',port=3306)
pool_project = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei', passwd='tanglei', db='commerce',port=3306)

pool_statistics = PooledDB(pymysql, 5, host='39.104.130.52', user='root', passwd='xmxc1234', db='statistics',
                        port=3520)

# 获取时间
up_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month - 1, 1).timetuple()))
to_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month, 1).timetuple()))
six_month_time = int(
    time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month - 6, 1).timetuple()))
last_month = datetime.date.today().month - 1

# 返回周边品类榜
@app.route('/api/getCate')
def get_cate():
    city_id = request.args.get('city_id')
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    cheak_key = '/api/getCate'+city_id + platform + coordinate + distance
    res = redis_conn.exists(cheak_key)
    if res ==1 :
        jsondatar=redis_conn.get(cheak_key)
    else:
        db = pool_mapmarkeronline.connection()
        cur = db.cursor()
        if city_id == '1':
            city = 'beijing'
        elif city_id == '2':
            city = 'shanghai'
        elif city_id == '3':
            city = 'hangzhou'
        elif city_id == '4':
            city = 'shenzhen'

        up_lat = float(lat) + 0.04
        down_lat = float(lat) - 0.04
        up_lng = float(lng) + 0.05
        down_lng = float(lng) - 0.05
        sql = "SELECT latitude,longitude,own_second_cate,month_sale_num FROM t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                   "'%s' and longitude BETWEEN '%s' and '%s' " % (platform, city, up_time, to_time, down_lat, up_lat, down_lng, up_lng)
        cur.execute(sql)
        results = cur.fetchall()

        key_value = []
        data_value = []
        month_sale_list = {}
        for row in results:
            latitude = row[0]
            longitude = row[1]
            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
            if dis <= float(distance):
                key_value.append(row[2])
                data_value.append(int(row[3]))
        month_sale_list['key'] = key_value
        month_sale_list['data'] = data_value
        df = pd.DataFrame(month_sale_list)
        kk = df.groupby(['key'], as_index=False)['data'].sum()
        kk_count = df.groupby(['key'], as_index=False)['data'].count()
        sq = []
        cate_name=[]
        cate_name1=[]
        cate_name2=[]
        cate_sum_list=[]
        cate_count_list=[]
        kk_values=kk.values
        kk_count_values=kk_count.values
        for a in kk_values:
            cate_name.append(a[0])
            cate_sum_list.append(a[1])
        zip1=zip(cate_name,cate_sum_list)
        sorted1=sorted(zip1,key=(lambda x:x[0]))
        for a in kk_count_values:
            cate_name1.append(a[0])
            cate_count_list.append(a[1])
        zip2=zip(cate_name1,cate_count_list)
        sorted2 = sorted(zip2,key=(lambda x:x[0]))

        if platform=='mt':
            key_ave = []
            data_ave = []
            ave_list = {}
            sql_ave = "SELECT latitude,longitude,own_second_cate,average_price FROM t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and average_price is not null and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                       "'%s' and longitude BETWEEN '%s' and '%s' " % (city, up_time, to_time, down_lat, up_lat, down_lng, up_lng)
            cur.execute(sql_ave)
            results_ave = cur.fetchall()
            for row in results_ave:
                latitude_ave = row[0]
                longitude_ave = row[1]
                dis_ave = getDistance(float(latitude_ave), float(longitude_ave), float(lat), float(lng))
                if dis_ave <= float(distance):
                    key_ave.append(row[2])
                    data_ave.append(int(row[3]))
            ave_list['key_ave'] = key_ave
            ave_list['data_ave'] = data_ave
            df = pd.DataFrame(ave_list)
            kk_ave = df.groupby(['key_ave'], as_index=False)['data_ave'].mean()

            cate_ave = []
            kk_ave_values=kk_ave.values
            for a in kk_ave_values:
                cate_name2.append(a[0])
                cate_ave.append(a[1])
            zip3=zip(cate_name2,cate_ave)
            sorted3=sorted(zip3,key=(lambda x:x[0]))

            sorted1.extend(sorted2)
            sorted1.extend(sorted3)

            d = dict()
            for item in sorted1:
                if item[0] in d:
                    d[item[0]].append(item[1])
                else:
                    d[item[0]] = [item[1]]

            res = []
            res2 = []
            for k, v in d.items():
                v.insert(0, k)
                res.append(v)
            for abc in res:
                if len(abc)<=3:
                    abc.append(0)
                res2.append(abc)
            res2 = sorted(res2, key=lambda x: x[2], reverse=True)
            for row in res2:
                all_cate = {}
                all_cate['cate_name'] = row[0]
                all_cate['cate_sum'] = row[1]
                all_cate['cate_count'] = row[2]
                all_cate['cate_ave'] = row[3]
                sq.append(all_cate)
            jsondatar = json.dumps(sq, ensure_ascii=False)
            redis_conn.set(cheak_key,jsondatar)
            redis_conn.expire(cheak_key, 2592000)
        elif platform=='elm':
            sorted1.extend(sorted2)
            d = dict()
            for item in sorted1:
                if item[0] in d:
                    d[item[0]].append(item[1])
                else:
                    d[item[0]] = [item[1]]

            res = []
            res2 = []
            for k, v in d.items():
                v.insert(0, k)
                res.append(v)
            for abc in res:
                if len(abc) <= 3:
                    abc.append(0)
                res2.append(abc)
            res2=sorted(res2,key=lambda x:x[1],reverse=True)

            for row in res2:
                all_cate = {}
                all_cate['cate_name'] = row[0]
                all_cate['cate_sum'] = row[1]
                all_cate['cate_count'] = row[2]
                sq.append(all_cate)
            jsondatar = json.dumps(sq, ensure_ascii=False)
            redis_conn.set(cheak_key, jsondatar)
            redis_conn.expire(cheak_key,2592000)
        db.close()
    redis_conn.close()
    return jsondatar



if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host="169.254.230.2", port=5001, debug=True)
