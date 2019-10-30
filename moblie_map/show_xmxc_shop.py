# coding:utf-8
import math

from DBUtils.PooledDB import PooledDB
from flask import Flask, request
import pymysql
import json
import time
import datetime
import pandas as pd

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


pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                                db='mapmarkeronline', port=62864)

pool_project = PooledDB(pymysql, 5, host='39.104.130.52', user='root', passwd='xmxc1234', db='commerce',
                        port=3520)

pool_statistics = PooledDB(pymysql, 5, host='39.104.130.52', user='root', passwd='xmxc1234', db='statistics',
                        port=3520)

# 获取时间
up_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month - 1, 1).timetuple()))
to_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month, 1).timetuple()))
six_month_time = int(
    time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month - 6, 1).timetuple()))
last_month = datetime.date.today().month - 1


# 返回写字楼数据
@app.route('/getOfficeInfo')
def getOfficeInfo():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    city = request.args.get('city')

    up_lat = float(lat) + 0.04
    down_lat = float(lat) - 0.04
    up_lng = float(lng) + 0.05
    down_lng = float(lng) - 0.05

    sql = "SELECT b_name,daily_rent,longitude,latitude from t_map_office_building where longitude !='不明' and b_city='%s'  and latitude BETWEEN '%s' and " \
          "'%s' and longitude BETWEEN '%s' and '%s'" % (city, down_lat, up_lat, down_lng, up_lng)
    sq = []
    cur.execute(sql)
    results = cur.fetchall()

    for row in results:
        data = {}
        b_name = row[0]
        daily_rent = row[1]
        longitude = row[2]
        latitude = row[3]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            # office_count=office_count+1
            data['office_name'] = b_name
            data['office_rent'] = daily_rent
            data['distance'] = dis
            sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回小区数据
@app.route('/getHousingInfo')
def getHousingInfo():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    city = request.args.get('city')
    up_lat = float(lat) + 0.04
    down_lat = float(lat) - 0.04
    up_lng = float(lng) + 0.05
    down_lng = float(lng) - 0.05
    sql = "SELECT uptown_name,SUBSTRING_INDEX(ave_price,'-',1) ave_price,longitude,latitude,house_num from t_map_lianjia_uptown where longitude !='不明' and " \
          "city='%s' and latitude BETWEEN '%s' and '%s' and longitude BETWEEN '%s' and '%s'"  % (city, down_lat, up_lat, down_lng, up_lng)
    sq = []
    cur.execute(sql)
    results = cur.fetchall()
    # 遍历结果
    for row in results:
        data = {}
        uptown_name = row[0]
        ave_price = row[1]
        longitude = row[2]
        latitude = row[3]
        house_num = row[4]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):

            data['office_name'] = uptown_name
            data['office_rent'] = ave_price
            data['house_num'] = house_num
            data['distance'] = dis
            sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回周边数据
@app.route('/getFoodDisInfo')
def getFoodDisInfo():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    city = request.args.get('city')
    up_lat = float(lat) + 0.04
    down_lat = float(lat) - 0.04
    up_lng = float(lng) + 0.05
    down_lng = float(lng) - 0.05
    sql_food = "SELECT latitude,longitude,client_name,month_sale_num from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and latitude BETWEEN '%s' and  " \
               "'%s' and longitude BETWEEN '%s' and '%s' order by month_sale_num desc" % (city, up_time, to_time, down_lat, up_lat, down_lng, up_lng)

    cur.execute(sql_food)
    results_food = cur.fetchall()
    food_info=[]
    for row in results_food:
        data={}
        dis = getDistance(float(row[0]), float(row[1]), float(lat), float(lng))
        if dis <= float(distance):
            data["shop_name"]=row[2]
            data["shop_month_sale"]=row[3]
            food_info.append(data)

    jsondatar = json.dumps(food_info, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回周边数据
@app.route('/getBaicInfo')
def getBaicInfo():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    city = request.args.get('city')
    up_lat = float(lat) + 0.04
    down_lat = float(lat) - 0.04
    up_lng = float(lng) + 0.05
    down_lng = float(lng) - 0.05
    city_name = ""
    if city == "beijing":
        city_name = "北京市"
    elif city == "shanghai":
        city_name = "上海市"
    elif city == "hangzhou":
        city_name = "杭州市"
    elif city == "shenzhen":
        city_name = "深圳市"
    sql_house = "SELECT longitude,latitude from t_map_lianjia_uptown where longitude !='不明' and city='%s'" % city_name
    sql_office = "SELECT longitude,latitude from t_map_office_building where longitude !='不明' and b_city='%s'" % city_name
    sql_food = "SELECT latitude,longitude from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and latitude BETWEEN '%s' and  " \
               "'%s' and longitude BETWEEN '%s' and '%s'" % (city, up_time, to_time, down_lat, up_lat, down_lng, up_lng)
    sq = []

    # 执行小区sql，获取数据
    cur.execute(sql_house)
    results_house = cur.fetchall()
    # 遍历结果
    house_count = 0
    all_data = {}

    for row in results_house:
        longitude = row[0]
        latitude = row[1]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):

            house_count = house_count + 1
    # 执行写字楼sql，获取数据
    cur.execute(sql_office)
    results_office = cur.fetchall()
    office_count = 0
    for row in results_office:
        longitude = row[0]
        latitude = row[1]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            office_count = office_count + 1


    # 获取餐饮信息
    cur.execute(sql_food)
    results_food = cur.fetchall()
    food_count = 0
    for row in results_food:
        dis = getDistance(float(row[0]), float(row[1]), float(lat), float(lng))
        if dis <= float(distance):
            food_count = food_count + 1


    # 获取医院个数
    sql_hospital = "SELECT hospital_lat,hospital_lng,hospital_name from t_map_hospital_info WHERE hospital_lat !='不明'"
    cur.execute(sql_hospital)
    results_hospital = cur.fetchall()
    hospital_count = 0
    hospital_info=[]
    for row_hos in results_hospital:
        hospital_data={}
        dis = getDistance(float(row_hos[0]), float(row_hos[1]), float(lat), float(lng))
        if dis <= float(distance):
            hospital_count += 1
            hospital_data['hospital_name']=row_hos[2]
            hospital_data['hospital_dis']=dis
            hospital_info.append(hospital_data)

    #获取学校信息
    sql_school = "SELECT school_lat,school_lng,school_name from t_map_school_info WHERE school_city='%s' and  school_lng !='不明'" %city_name
    cur.execute(sql_school)
    results_school = cur.fetchall()
    school_count = 0
    school_info=[]
    for row_hos in results_school:
        school_data={}
        dis = getDistance(float(row_hos[0]), float(row_hos[1]), float(lat), float(lng))
        if dis <= float(distance):
            school_count += 1
            school_data["school_name"]=row_hos[2]
            school_data["school_dis"]=dis
            school_info.append(school_data)
    #获取酒店、商场信息
    city_name2=city_name[0:2]
    sql_buildings="SELECT buildings_latitude,buildings_longitude,total_type,buildings_name from t_map_buildings where city_name='%s'" %city_name2
    cur.execute(sql_buildings)
    results_buildings = cur.fetchall()
    market_count = 0
    hotel_count=0
    market_info=[]
    hotel_info=[]
    for row_hos in results_buildings:
        dis = getDistance(float(row_hos[0]), float(row_hos[1]), float(lat), float(lng))
        market_data={}
        hotel_data={}
        if dis <= float(distance):
            if row_hos[2]=='商场':
                market_count += 1
                market_data["market_name"]=row_hos[3]
                market_data["market_dis"]=dis
                market_info.append(market_data)
            elif row_hos[2]=='酒店':
                hotel_count+=1
                hotel_data['hotel_name']=row_hos[3]
                hotel_data['hotel_dis']=dis
                hotel_info.append(hotel_data)

    # 整和数据返回
    baic_info={}
    baic_info['house_count'] = house_count
    baic_info['office_count'] = office_count
    baic_info['food_count'] = food_count
    baic_info['hospital_count'] = hospital_count
    baic_info['school_count'] = school_count
    baic_info['market_count'] = market_count
    baic_info['hotel_count'] = hotel_count

    all_data['hotel_info']=hotel_info
    all_data['market_info']=market_info
    all_data['baic_info']=baic_info
    all_data['school_info']=school_info
    all_data['hospital_info']=hospital_info

    sq.append(all_data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回月销量统计
@app.route('/getShopBasic')
def getGreensFrom():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city = request.args.get('city')
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    sql = "SELECT month_sale_num,latitude,longitude from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康')" % (
    platform, city, up_time, to_time)
    cur.execute(sql)
    results = cur.fetchall()
    sq = []
    city_sale_money = 0
    dis_sale_money = 0
    dis_sale_count = 0
    data = {}
    for row in results:
        month_sale = int(row[0])
        latitude: str = row[1]
        longitude = row[2]
        # print(longitude)
        city_sale_money = city_sale_money + month_sale
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            dis_sale_money = dis_sale_money + month_sale
            dis_sale_count = dis_sale_count + 1
    dis_ave_shop_sale = dis_sale_money / dis_sale_count
    data['city_sale_money'] = city_sale_money
    data['dis_sale_money'] = dis_sale_money
    data['dis_sale_count'] = dis_sale_count
    data['dis_ave_shop_sale'] = dis_ave_shop_sale
    sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回人均
@app.route('/getAveMoney')
def getAveMoney():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city = request.args.get('city')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    sql = "SELECT average_price,latitude,longitude from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康')" % (
    city, up_time, to_time)
    cur.execute(sql)
    results = cur.fetchall()
    sq = []
    city_sale_money = 0
    city_sale_count = 0
    dis_sale_money = 0
    dis_sale_count = 0
    data = {}
    for row in results:
        average_price = row[0]
        latitude: str = row[1]
        longitude = row[2]
        # print(longitude)
        if average_price is None or average_price.strip() == '':
            average_price = '0'

        city_sale_money = city_sale_money + average_price
        city_sale_count = city_sale_count + 1
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            dis_sale_money = dis_sale_money + average_price
            dis_sale_count = dis_sale_count + 1

    dis_ave_shop_sale = dis_sale_money / dis_sale_count
    city_ave_shop_sale = city_sale_money / city_sale_count
    data['city_ave_shop_sale'] = city_ave_shop_sale
    data['dis_ave_shop_sale'] = dis_ave_shop_sale
    sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回周边品类榜
@app.route('/getCate')
def getMoney():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city = request.args.get('city')
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    sql = "SELECT latitude,longitude,own_second_cate,month_sale_num FROM t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') " % (
    platform, city, up_time, to_time)
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
    sq = []
    all_cate = {}
    cate_sum = {}
    for a in kk.values:
        cate_sum[a[0]] = a[1]

    key_ave = []
    data_ave = []
    ave_list = {}
    sql_ave = "SELECT latitude,longitude,own_second_cate,average_price FROM t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and average_price is not null and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') " % (
    city, up_time, to_time)
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

    cate_ave = {}
    for a in kk_ave.values:
        cate_ave[a[0]] = a[1]

    all_cate['cate_sum'] = cate_sum
    all_cate['cate_ave'] = cate_ave

    sq.append(all_cate)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回菜品销量榜
@app.route('/getFood')
def getFood():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city = request.args.get('city')
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    cate = request.args.get('cate')
    sql = "SELECT latitude,longitude,shop_id,update_count FROM t_map_client_%s_%s_mark where update_time BETWEEN %s and %s  and own_second_cate='%s' and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') " % (
    platform, city, up_time, to_time, cate)
    cur.execute(sql)
    results = cur.fetchall()
    update_count = results[0][3]
    str_shop_id = ''
    for row in results:
        latitude = row[0]
        longitude = row[1]

        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            shop_id = row[2]
            str_shop_id = str_shop_id + "'" + shop_id + "',"
    str_shop_id = str_shop_id.strip(',')

    sql_food = "SELECT food_name,SUM(month_sale) sum_sale from t_map_client_%s_%s_mark_food where month_sale is not null and update_count=%s and client_id in (%s) GROUP BY food_name order by sum_sale desc limit 50" % (
    platform, city, update_count, str_shop_id)
    cur.execute(sql_food)
    results_food = cur.fetchall()
    sq = []
    food_dicr = {}
    for a in results_food:
        food_dicr[a[0]] = str(a[1])
    sq.append(food_dicr)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回销售趋势
@app.route('/getLineChart')
def getLineChart():
    db = pool_mapmarkeronline.connection()
    sta_db=pool_statistics.connection()
    cur_sta=sta_db.cursor()
    cur = db.cursor()
    city = request.args.get('city')
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    project_id = request.args.get('project_id')
    shop_to_time = datetime.date(datetime.date.today().year, datetime.date.today().month - 6, 1).strftime('%Y%m%d')
    shop_end_time = str(datetime.date(datetime.date.today().year,datetime.date.today().month,1)-datetime.timedelta(1)).replace('-','')
    get_update_sql = "SELECT update_count FROM t_map_client_%s_%s_mark where update_time BETWEEN %s and %s limit 1" % (
        platform, city, up_time, to_time)
    cur.execute(get_update_sql)
    results_get_update = cur.fetchall()
    last_month_update_count = results_get_update[0][0]
    print(last_month_update_count)
    six_month_update_count = last_month_update_count - 5
    get_city_sql = "SELECT update_count,AVG(month_sale_num) from t_map_client_%s_%s_mark where update_count between %s and %s   and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') GROUP BY update_count" % (
    platform, city, six_month_update_count,last_month_update_count)
    cur.execute(get_city_sql)
    results_city = cur.fetchall()
    city_data = {}
    for row in results_city:

        if platform == 'elm':
            city_month = str(int(row[0]) - 3) + '月'
        else:
            city_month = str(row[0]) + '月'

        city_data[city_month] = str(row[1])

    up_lat = float(lat) + 0.04
    down_lat = float(lat) - 0.04
    up_lng = float(lng) + 0.05
    down_lng = float(lng) - 0.05

    get_dis_sql = "SELECT update_count,month_sale_num,latitude,longitude from t_map_client_%s_%s_mark where update_count between %s and %s   and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康')  and latitude BETWEEN '%s' and '%s' and longitude BETWEEN '%s' and '%s'" \
                  % (platform, city, six_month_update_count,last_month_update_count, down_lat, up_lat, down_lng, up_lng)
    cur.execute(get_dis_sql)
    results_dis = cur.fetchall()
    dis_month = []
    dis_month_sale = []
    dis_dict = {}
    for row in results_dis:
        if platform == 'elm':
            city_month = str(int(row[0]) - 3) + '月'
        else:
            city_month = str(row[0]) + '月'
        latitude = row[2]
        longitude = row[3]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            dis_month.append(city_month)
            dis_month_sale.append(row[1])

    dis_dict['key'] = dis_month
    dis_dict['data'] = dis_month_sale
    df = pd.DataFrame(dis_dict)
    kk = df.groupby(['key'], as_index=False)['data'].mean()

    sql_shop="SELECT a.project_id, SUM(a.sum_order_count), COUNT(1), RIGHT(a.date,2)  FROM ( SELECT  project_id,  merchant_name,  SUM(order_count) sum_order_count, " \
             " LEFT (stream_date, 6) date FROM  merchant_statistics WHERE  project_id = %s AND stream_date BETWEEN '%s' and '%s' GROUP BY  merchant_id," \
             "LEFT (stream_date, 6) ) a GROUP BY a.date" %(project_id,shop_to_time,shop_end_time)
    cur_sta.execute(sql_shop)
    results_cur=cur_sta.fetchall()
    shop_dict={}
    for row in results_cur:
        shop_ave=0.0
        if int(row[2]>0):
            shop_ave=float(row[1])/int(row[2])
        shop_data=str(int(row[3]))+'月'
        shop_dict[shop_data]=shop_ave

    sq = []
    all_data = {}
    dis_data = {}
    for a in kk.values:
        dis_data[a[0]] = a[1]

    all_data['city_data'] = city_data
    all_data['dis_data'] = dis_data
    all_data['shop_data'] = shop_dict

    sq.append(all_data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


@app.route('/getBaicDetails')
def getBaicDetails():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    city = request.args.get('city')
    sql_food = "SELECT own_second_cate,month_sale_num,latitude,longitude from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and " \
               "own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康')" % (platform, city, up_time, to_time)
    cur.execute(sql_food)
    results = cur.fetchall()
    cate_name_list = []
    cate_sum_list = []
    cate_dict = {}
    for row in results:

        latitude = row[2]
        longitude = row[3]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            cate_name_list.append(row[0])
            cate_sum_list.append(row[1])
    cate_dict['key'] = cate_name_list
    cate_dict['data'] = cate_sum_list
    df = pd.DataFrame(cate_dict)
    kk_sum = df.groupby(['key'], as_index=False)['data'].sum()
    kk_count = df.groupby(['key'], as_index=False)['data'].count()

    sq = []
    all_data = {}
    dis_sum = {}
    dis_count = {}
    for a in kk_sum.values:
        dis_sum[a[0]] = a[1]
    for a in kk_count.values:
        dis_count[a[0]] = a[1]
    all_data['count'] = dis_count
    all_data['sum'] = dis_sum
    sq.append(all_data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 获得品类下详细菜品
@app.route('/getCateShop')
def getCateShop():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    platform = request.args.get('platform')
    cate = request.args.get('cate')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    city = request.args.get('city')
    sql = "SELECT client_name,month_sale_num,latitude,longitude from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') " \
          "and own_second_cate='%s' order by month_sale_num desc" % (platform, city, up_time, to_time, cate)
    cur.execute(sql)
    reultes = cur.fetchall()
    sql_list = []
    for row in reultes:
        data = {}
        latitude = row[2]
        longitude = row[3]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            data['shop_name'] = row[0]
            data['month_sale'] = row[1]
            sql_list.append(data)
    jsondatar = json.dumps(sql_list, ensure_ascii=False)
    db.close()
    return jsondatar


# 获取竞对数据
@app.route('/getRace')
def getRace():
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    sql = "SELECT mark_name,latitude,longitude,area,stall_num,seat_num,month_rent,entry_fee from t_map_mark"
    cur.execute(sql)
    results = cur.fetchall()

    sq = []
    for row in results:
        data = {}
        latitude = row[1]
        longitude = row[2]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))

        if dis <= float(distance):
            data['mark_name'] = row[0]
            data['area'] = row[3]
            data['stall_num'] = row[4]
            data['seat_num'] = row[5]
            data['month_rant'] = row[6]
            data['entry_fee'] = row[7]
            sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回项目列表
@app.route('/getProjectList')
def getProjectList():
    db = pool_project.connection()
    cur = db.cursor()
    sql = "SELECT project_id,project_name from project where status=2"
    cur.execute(sql)
    results = cur.fetchall()
    sq = []
    for row in results:
        data = {}
        project_name = row[1]
        project_id = row[0]
        data['project_id'] = project_id
        data['project_name'] = project_name
        sq.append(data)
    jsondu = json.dumps(sq, ensure_ascii=False)
    pool_project.close()
    return jsondu


# 返回档口统计数据
@app.route('/getStalls')
def getStalls():
    db = pool_project.connection()
    cur = db.cursor()
    project_id = request.args.get("project_id")
    sql = "SELECT status,COUNT(1)  from stalls where project_id=%s  GROUP BY status" % project_id
    cur.execute(sql)
    results = cur.fetchall()
    start_time = getdate(30, '%Y-%m-%d')
    end_time = getdate(1, '%Y-%m-%d')
    on_business = 0
    empty = 0
    for row in results:
        status = row[0]
        status_count = row[1]
        if status == 0 or status == 6:
            on_business = on_business + status_count
        elif status == 3 or status == 5:
            empty = empty + status_count
    sql_new_shop = "SELECT COUNT(1) from contract where project_id=%s and enter_time  BETWEEN '%s' and '%s' GROUP BY stall_id" % (
    project_id, start_time, end_time)
    cur.execute(sql_new_shop)
    results_new_shop = cur.fetchall()
    new_shop = 0
    if len(results_new_shop) > 0:
        new_shop = results_new_shop[0][0]

    data = {}
    data['on_business'] = on_business
    data['empty'] = empty
    data['new_shop'] = new_shop
    sq = []
    sq.append(data)
    db.close()
    jsondu = json.dumps(sq, ensure_ascii=False)
    return jsondu


#返回商户信息
@app.route('/getXmxcShop')
def getXmxcShop():
    db=pool_statistics.connection()
    cur=db.cursor()
    project_id=request.args.get("project_id")
    start_time = getdate(30, '%Y%m%d')
    end_time = getdate(1, '%Y%m%d')
    sql="SELECT merchant_id,merchant_name,SUM(order_count),SUM(sale_amount) from merchant_statistics where project_id=%s and  stream_date BETWEEN '%s' and '%s' GROUP BY merchant_id" % (project_id,start_time,end_time)

    cur.execute(sql)
    results=cur.fetchall()
    all_json=[]
    sq=[]
    all_count=0
    all_money=0.0
    for row in results:
        order_count = int(row[2])
        sale_amount = float(row[3])
        all_count += order_count
        all_money += sale_amount

    for row in results:
        data={}
        merchant_name=row[1]
        order_count=int(row[2])
        proportion=0.0
        if order_count>0:
            proportion=order_count/all_count
        data['merchant_name']=merchant_name
        data['order_count']=order_count
        data['proportion']=proportion
        sq.append(data)
    ave_shop=0
    if all_count>0:
        ave_shop=all_money/all_count

    sql_cate = "select (select mcc.category_name from commerce.merchants_configuration_category mcc where mcc.tid = mcr.second_category_id),sum(ms.sale_amount)," \
               "sum(ms.order_count) from merchant_statistics ms left join merchant_category_rela mcr on ms.merchant_id = mcr.merchant_id where ms.project_id =" \
               " %s and mcr.second_category_id != 0 and ms.stream_date between '%s' and '%s' group by mcr.second_category_id" % (
    project_id, start_time, end_time)
    print(sql_cate)
    cur.execute(sql_cate)
    results_cate = cur.fetchall()
    cate_list=[]
    for row in results_cate:
        cata_data={}
        cate_name=row[0]
        cate_count=row[2]
        cate_ave=0.0
        if cate_count>0:
            cate_ave=cate_count/all_count
        cata_data["cate_name"]=cate_name
        cata_data["cate_count"]=str(cate_count)
        cata_data["cate_ave"]=str(cate_ave)
        cate_list.append(cata_data)
    json_dict={}
    json_dict['all_order_count']=str(all_count)
    json_dict['all_all_money']=str(all_money)
    json_dict['ave_shop']=str(ave_shop)
    json_dict['shop_list']=sq
    json_dict['cate_list']=cate_list
    all_json.append(json_dict)

    db.close()
    jsondu=json.dumps(all_json,ensure_ascii=False)
    return jsondu


if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host="127.0.0.1", port=5000, debug=True)
