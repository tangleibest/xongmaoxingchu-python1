# coding:utf-8
import math

from DBUtils.PooledDB import PooledDB
from flask import Flask, request,Blueprint
from flask_docs import ApiDoc
import pymysql
from flask_cors import *
import json
import time
import datetime
import pandas as pd

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.config['API_DOC_MEMBER'] = ['api', 'platform']

ApiDoc(app)

api = Blueprint('api', __name__)
platform = Blueprint('platform', __name__)

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

pool_project = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei', passwd='tanglei', db='commerce',
                        port=3306)

pool_statistics = PooledDB(pymysql, 5, host='39.104.130.52', user='root', passwd='xmxc1234', db='statistics',
                        port=3520)

# 获取时间
up_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month - 1, 1).timetuple()))
to_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month, 1).timetuple()))
six_month_time = int(
    time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month - 6, 1).timetuple()))
last_month = datetime.date.today().month - 1


# 返回写字楼数据
@app.route('/api/getOfficeInfo')
def get_office_info():
    """返回写字楼详细列表

        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
        |    distance    |    半径    |    string   |    1.5    |
        |    city    |    城市    |    string   |    北京市   |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    office_name    |    写字楼名称    |    string   |
        |    distance    |    与所选点距离    |    double   |

        #### return
        - ##### json
        > [{"office_name": "恒通商务园", "office_rent": "5.5", "distance": 0.3893995993948515}, {"office_name": "瀚海国际大厦 ", "office_rent": "6.9", "distance": 0.5507685602422158}]
        @@@
        """
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

    sql = "SELECT b_name,longitude,latitude from t_map_office_building where longitude !='不明' and b_city='%s'  and latitude BETWEEN '%s' and " \
          "'%s' and longitude BETWEEN '%s' and '%s'" % (city, down_lat, up_lat, down_lng, up_lng)
    sq = []
    cur.execute(sql)
    results = cur.fetchall()

    for row in results:
        data = {}
        b_name = row[0]
        longitude = row[1]
        latitude = row[2]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            # office_count=office_count+1
            data['office_name'] = b_name
            data['distance'] = dis
            sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回小区数据
@app.route('/api/getHousingInfo')
def get_housing_info():
    """返回小区详细数据列表

            @@@
            #### 参数列表

            | 参数 | 描述 | 类型 | 例子 |
            |--------|--------|--------|--------|
            |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
            |    distance    |    半径    |    string   |    1.5    |
            |    city    |    城市    |    string   |    北京市   |

            #### 字段解释

            | 名称 | 描述 | 类型 |
            |--------|--------|--------|--------|
            |    housing_name    |    小区名称    |    string   |
            |    distance    |    与所选点距离    |    double   |

            #### return
            - ##### json
            > [{"housing_name": "芳园里", "distance": 0.5681041154328037}, {"housing_name": "银河湾", "distance": 0.8682477990692311}]
            @@@
            """
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
    sql = "SELECT uptown_name,longitude,latitude from t_map_lianjia_uptown where longitude !='不明' and " \
          "city='%s' and latitude BETWEEN '%s' and '%s' and longitude BETWEEN '%s' and '%s'"  % (city, down_lat, up_lat, down_lng, up_lng)
    sq = []
    cur.execute(sql)
    results = cur.fetchall()
    # 遍历结果
    for row in results:
        data = {}
        uptown_name = row[0]
        longitude = row[1]
        latitude = row[2]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):

            data['housing_name'] = uptown_name

            data['distance'] = dis
            sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回周边配套餐饮商户名单
# @app.route('/api/getFoodDisInfo')
# def get_food_dis_info():
#     """返回周边配套餐饮商户名单
#
#     @@@
#     #### 参数列表
#
#     | 参数 | 描述 | 类型 | 例子 | 备注 |
#     |--------|--------|--------|--------|--------|
#     |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |       |
#     |    distance    |    半径    |    string   |    1.5    |       |
#     |    city    |    城市    |    string   |    beijing   |       |
#     |    cate    |    品类    |    string   |    东北菜   |    需要从周边品类榜中获取   |
#
#     #### 字段解释
#
#     | 名称 | 描述 | 类型 |
#     |--------|--------|--------|--------|
#     |    shop_name    |    商户名称    |    string   |
#     |    shop_month_sale    |    商户月销量    |    int   |
#
#     #### return
#     - ##### json
#     > [{"shop_name": "川湘快餐（第1档口+呱呱美食城店）", "shop_month_sale": 9999}, {"shop_name": "张亮麻辣烫（将台路店）", "shop_month_sale": 7645}]
#     @@@
#     """
#     db = pool_mapmarkeronline.connection()
#     cur = db.cursor()
#     coordinate = request.args.get('coordinate')
#     lat = str(coordinate).split(",")[1]
#     lng = str(coordinate).split(",")[0]
#     distance = request.args.get('distance')
#     city = request.args.get('city')
#     cate = request.args.get('cate')
#     up_lat = float(lat) + 0.04
#     down_lat = float(lat) - 0.04
#     up_lng = float(lng) + 0.05
#     down_lng = float(lng) - 0.05
#     sql_food = "SELECT latitude,longitude,client_name,month_sale_num,own_second_cate from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康')  and latitude BETWEEN '%s' and  " \
#                "'%s' and longitude BETWEEN '%s' and '%s' and own_second_cate='%s' order by month_sale_num desc" % (city, up_time, to_time, down_lat, up_lat, down_lng, up_lng,cate)
#
#     cur.execute(sql_food)
#     results_food = cur.fetchall()
#     food_info=[]
#     for row in results_food:
#         data={}
#         dis = getDistance(float(row[0]), float(row[1]), float(lat), float(lng))
#         if dis <= float(distance):
#             data["shop_name"]=row[2]
#             data["shop_month_sale"]=row[3]
#             food_info.append(data)
#
#     jsondatar = json.dumps(food_info, ensure_ascii=False)
#     db.close()
#     return jsondatar


# 返回周边数据下周边配套数据

@app.route('/api/getBaicInfo')
def get_baic_info():
    """返回周边数据下周边配套数据及商场、酒店、高校、医院详情

        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
        |    distance    |    半径    |    string   |    1.5    |
        |    city    |    城市    |    string   |    beijing   |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    baic_info    |    周边配套数量统计    |    object   |
        |    └house_count    |    小区数量    |    int   |
        |    └office_count    |    写字楼数量    |    int   |
        |    └food_count    |    餐饮数量    |    int   |
        |    └hospital_count    |    医院数量    |    int   |
        |    └school_count    |    学校数量    |    int   |
        |    └market_count    |    商场数量    |    int   |
        |    └hotel_count    |    酒店数量    |    int   |
        |    hotel_info[]    |    酒店列表    |    list   |
        |    └hotel_name    |    酒店名称    |    string   |
        |    └hotel_dis   |    酒店与所选点距离    |    double   |
        |    market_info[]   |    商场列表    |    list   |
        |    └market_name   |    商场名称    |    string   |
        |    └market_dis   |    商场与所选点距离    |    double   |
        |    school_info[]   |    学校列表    |    list   |
        |    └school_name   |    学校名称    |    string   |
        |    └school_dis   |    学校与所选点距离    |    double   |

        #### return
        - ##### json
        > [{"hotel_info": [{{"hotel_name": "秋果酒店(798艺术区店)", "hotel_dis": 1.3007605484401805}, {"hotel_name": "北京酒仙酒店公寓", "hotel_dis": 1.1329057421469126}], "market_info": [{"market_name": "颐堤港", "market_dis": 0.7547364429446455}{"market_name": "北京华联(颐堤港超市)", "market_dis": 0.7700639203387806}], "baic_info": {"house_count": 66, "office_count": 48, "food_count": 505, "hospital_count": 5, "school_count": 1, "market_count": 12, "hotel_count": 35}, "school_info": [{"school_name": "北京信息职业技术学院", "school_dis": 1.0214379968385598}], "hospital_info": [{"hospital_name": "朝阳区将台地区将府家园社区卫生服务站", "hospital_dis": 0.8824207876096655}]}]
        @@@
        """
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

    all_data['baic_info'] = baic_info
    all_data['hotel_info']=hotel_info
    all_data['market_info']=market_info
    all_data['school_info']=school_info
    all_data['hospital_info']=hospital_info

    sq.append(all_data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回月销量统计
@app.route('/api/getShopBasic')
def get_shop_basic():
    """返回写字楼详细列表

            @@@
            #### 参数列表

            | 参数 | 描述 | 类型 | 例子 |
            |--------|--------|--------|--------|
            |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
            |    distance    |    半径    |    string   |    1.5    |
            |    city    |    城市    |    string   |    beijing   |
            |    platform    |    外卖平台（mt、elm）    |    string   |    elm   |

            #### 字段解释

            | 名称 | 描述 | 类型 |
            |--------|--------|--------|--------|
            |    city_sale_money    |    全城销量总数    |    int   |
            |    dis_sale_money    |    区域商铺总数    |    int   |
            |    dis_sale_count    |    区域销量总数   |    int   |
            |    dis_ave_shop_sale    |    区域单店均销量    |    double   |

            #### return
            - ##### json
            > [{"city_sale_money": 37508086, "dis_sale_money": 482470, "dis_sale_count": 422, "dis_ave_shop_sale": 1143.2938388625591}]
            @@@
            """
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
@app.route('/api/getAveMoney')
def get_ave_money():
    """返回餐饮人均消费及月销量统计下的区域客单价
        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
        |    distance    |    半径    |    string   |    1.5    |
        |    city    |    城市    |    string   |    beijing   |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    city_ave_shop_sale    |    北京人均    |    double   |
        |    dis_ave_shop_sale    |    周边人均    |    double   |

        #### return
        - ##### json
        > [{"city_ave_shop_sale": 23.536748880587783, "dis_ave_shop_sale": 26.49526066350711}]
        @@@
        """
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
        average_price=float(average_price)
        city_sale_money = city_sale_money + float(average_price)
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
@app.route('/api/getCate')
def get_cate():
    """返回周边品类榜

        @@@
        #### 参数列表

        | 参数 | 参数解释 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
        |    distance    |    半径    |    string   |    1.5    |
        |    city    |    城市    |    string   |    北京市   |
        |    platform    |    外卖平台（mt、elm）    |    string   |    elm   |

        #### 字段解释

        | 名称 | 解释 | 类型 |
        |--------|--------|--------|--------|
        |    cate_name    |    品类名称    |    string   |
        |    cate_sum    |    该品类销售总单量    |    int   |
        |    cate_count    |    该品类下商户数    |    int   |
        |    cate_ave    |    该品类客单价    |    double   |

        #### return
        - ##### json
        > [{"cate_name": "东北菜", "cate_sum": 18043, "cate_count": 10, "cate_ave": 27.8}, {"cate_name": "云南菜", "cate_sum": 1384, "cate_count": 3, "cate_ave": 40.0}]
        @@@
        """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city = request.args.get('city')
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
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
    cate_sum_list=[]
    cate_count_list=[]
    for a in kk.values:
        cate_name.append(a[0])
        cate_sum_list.append(a[1])
    for a in kk_count.values:
        cate_count_list.append(a[1])

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
    for a in kk_ave.values:
        cate_ave.append(a[1])

    zipd = list(zip(cate_name, cate_sum_list, cate_count_list,cate_ave))
    print(zipd)
    v = sorted(zipd, key=(lambda x: x[1]), reverse=True)
    for row in v:
        all_cate = {}
        all_cate['cate_name'] = row[0]
        all_cate['cate_sum'] = row[1]
        all_cate['cate_count'] = row[2]
        all_cate['cate_ave'] = row[3]
        sq.append(all_cate)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回菜品销量榜
@app.route('/api/getFood')
def get_food():
    """返回菜品月度销量榜

        @@@
        #### 参数列表

        | 参数 | 参数解释 | 类型 | 例子 | 备注 |
        |--------|--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |       |
        |    distance    |    半径    |    string   |    1.5    |       |
        |    city    |    城市    |    string   |    beijing   |       |
        |    platform    |    外卖平台（mt、elm）    |    string   |    elm   |       |
        |    cate    |    品类    |    string   |    东北菜   |    需要从周边品类榜中获取   |

        #### 字段解释

        | 名称 | 解释 | 类型 |
        |--------|--------|--------|--------|
        |    food_name    |    菜品名称    |    string   |
        |    food_sale_num    |    菜品销量    |    string   |


        #### return
        - ##### json
        > [{"food_name": "春饼", "food_sale_num": "24000"}, {"food_name": "玉米渣粥", "food_sale_num": "14000"}]
        @@@
        """
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

    for a in results_food:
        food_dicr = {}
        food_dicr['food_name']=a[0]
        food_dicr['food_sale_num']=str(a[1])
        sq.append(food_dicr)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回销售趋势
@app.route('/api/getLineChart')
def get_line_chart():
    """返回周边数据、门店数据下销售趋势

        @@@
        #### 参数列表

        | 参数 | 参数解释 | 类型 | 例子 | 备注 |
        |--------|--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |       |
        |    distance    |    半径    |    string   |    1.5    |       |
        |    city    |    城市    |    string   |    北京市   |       |
        |    platform    |    外卖平台（mt、elm）    |    string   |    elm   |       |
        |    project_id    |    项目id    |    string   |    东北菜   |    需要从周边品类榜中获取   |

        #### 字段解释

        | 名称 | 解释 | 类型 |
        |--------|--------|--------|--------|
        |    month    |    月份    |    string   |
        |    city_sale_num    |    全城单商户月均销量    |    string   |
        |    dis_sale_num    |    区域单商户月均销量  |    doublo   |
        |    dis_sale_num    |    门店单商户月均销量    |    double   |

        #### return
        - ##### json
        > [{"month": "4月", "city_sale_num": "719.5404", "dis_sale_num": 862.672932330827, "xmxc_sale_num": 4902.0}]
        @@@
        """

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
    six_month_update_count = last_month_update_count - 5
    get_city_sql = "SELECT update_count,AVG(month_sale_num) from t_map_client_%s_%s_mark where update_count between %s and %s   and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') GROUP BY update_count" % (
    platform, city, six_month_update_count,last_month_update_count)
    cur.execute(get_city_sql)
    results_city = cur.fetchall()
    month_list=[]
    city_list=[]
    dis_shop_list=[]

    for row in results_city:

        if platform == 'elm':
            city_month = str(int(row[0]) - 3) + '月'
        else:
            city_month = str(row[0]) + '月'
        month_list.append(city_month)
        city_list.append(str(row[1]))

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

    xmxc_list = []
    for row in results_cur:
        shop_ave=0.0
        if int(row[2]>0):
            shop_ave=float(row[1])/int(row[2])
        xmxc_list.append(shop_ave)

    sq = []

    for a in kk.values:
        dis_shop_list.append(a[1])
    da=list(zip(month_list,city_list,dis_shop_list,xmxc_list))

    for a in da:
        all_data = {}
        all_data['month']=a[0]
        all_data['city_sale_num']=a[1]
        all_data['dis_sale_num']=a[2]
        all_data['xmxc_sale_num']=a[3]
        sq.append(all_data)

    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar

#返回周边数据、门店数据下销售趋势
@app.route('/api/getBaicDetails')
def get_baic_details():
    """返回周边数据、门店数据下销售趋势

            @@@
            #### 参数列表

            | 参数 | 参数解释 | 类型 | 例子 | 备注 |
            |--------|--------|--------|--------|--------|
            |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |       |
            |    distance    |    半径    |    string   |    1.5    |       |
            |    city    |    城市    |    string   |    北京市   |       |
            |    platform    |    外卖平台（mt、elm）    |    string   |    elm   |       |
            |    project_id    |    项目id    |    string   |    1548233981574173   |    需要从场地列表获取   |

            #### 字段解释

            | 名称 | 解释 | 类型 |
            |--------|--------|--------|--------|
            |    month    |    月份    |    string   |
            |    city_sale_num    |    全城单商户月均销量    |    string   |
            |    dis_sale_num    |    区域单商户月均销量  |    doublo   |
            |    dis_sale_num    |    门店单商户月均销量    |    double   |

            #### return
            - ##### json
            > [{"month": "4月", "city_sale_num": "719.5404", "dis_sale_num": 862.672932330827, "xmxc_sale_num": 4902.0}]
            @@@
    """
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
@app.route('/api/getCateShop')
def get_cate_shop():
    """返回周边配套餐饮商户名单

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 | 备注 |
    |--------|--------|--------|--------|--------|
    |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |       |
    |    distance    |    半径    |    string   |    1.5    |       |
    |    city    |    城市    |    string   |    beijing   |       |
    |    cate    |    品类    |    string   |    东北菜   |    需要从周边品类榜中获取   |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    shop_name    |    商户名称    |    string   |
    |    shop_month_sale    |    商户月销量    |    int   |
    |    average_price    |    人均    |    string   |

    #### return
    - ##### json
    > [{"shop_name": "川湘快餐（第1档口+呱呱美食城店）", "shop_month_sale": 9999}, {"shop_name": "张亮麻辣烫（将台路店）", "shop_month_sale": 7645}]
    @@@
    """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    cate = request.args.get('cate')
    coordinate = request.args.get('coordinate')
    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    distance = request.args.get('distance')
    city = request.args.get('city')
    sql = "SELECT client_name,month_sale_num,latitude,longitude,average_price from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') " \
          "and own_second_cate='%s' order by month_sale_num desc" % (city, up_time, to_time, cate)
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
            data['average_price'] = row[2]
            sql_list.append(data)
    jsondatar = json.dumps(sql_list, ensure_ascii=False)
    db.close()
    return jsondatar


# 获取竞对数据
@app.route('/api/getRace')
def get_race():
    """获取竞对数据

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 |
    |--------|--------|--------|--------|
    |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
    |    distance    |    半径    |    string   |    1.5    |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    mark_name    |    商户名称    |    string   |
    |    area    |    面积    |    string   |
    |    stall_num    |    档口数    |    string   |
    |    seat_num    |    座位数    |    string   |
    |    month_rant    |    档口租金    |    string   |
    |    entry_fee    |    进场费    |    string   |

    #### return
    - ##### json
    > [{"mark_name": "餐行者美食广场", "area": null, "stall_num": 19, "seat_num": "10", "month_rant": "8000-08-25 00:00:00", "entry_fee": "20000/8-25"}]
    @@@
    """
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
@app.route('/api/getProjectList')
def get_project_list():
    """获取门店列表

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 |
    |--------|--------|--------|--------|
    |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）   |   string    |   1    |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    project_id    |    项目id    |    long   |
    |    project_name    |    项目名称    |    string   |
    |    address    |    地址    |    string   |
    |    latitude    |    纬度    |    string   |
    |    longitude    |    经度    |    string   |

    #### return
    - ##### json
    > [{"project_id": 1548233981349868, "project_name": "自空间", "address": "北京市朝阳区石门东路", "latitude": "39.9028700000000000", "longitude": "116.5030800000000000"}]
    @@@
    """
    db = pool_project.connection()
    cur = db.cursor()
    city_id = request.args.get('city_id')
    city_name=''
    if city_id=='1':
        city_name='北京'
    elif city_id=='2':
        city_name='上海'
    elif city_id=='3':
        city_name='杭州'
    elif city_id=='4':
        city_name='深圳'
    sql = "SELECT a.project_id,a.project_name,b.address,b.latitude,b.longitude from project a LEFT JOIN development.project_base_info b on a.project_id=b.tid WHERE a.area_name='%s'" %city_name

    cur.execute(sql)
    results = cur.fetchall()
    sq = []
    for row in results:
        data = {}
        project_name = row[1]
        project_id = row[0]
        address = row[2]
        latitude = str(row[3])
        longitude = str(row[4])
        data['project_id'] = project_id
        data['project_name'] = project_name
        data['address'] = address
        data['latitude'] = latitude
        data['longitude'] = longitude
        sq.append(data)
    jsondu = json.dumps(sq, ensure_ascii=False)
    pool_project.close()
    return jsondu


# 返回档口统计数据
@app.route('/api/getStalls')
def get_stalls():
    """查看项目下档口统计

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 |
    |--------|--------|--------|--------|
    |    project_id    |    项目id    |    string   |    1548233985051132    |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    on_business    |    营业中档口    |    int   |
    |    empty    |    空档口    |    int   |
    |    empty    |    新商户    |    int   |


    #### return
    - ##### json
    > [{"on_business": 10, "empty": 2, "new_shop": 0}]
    @@@
    """
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
@app.route('/api/getXmxcShop')
def get_xmxc_shop():
    """查看门店数据

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 |
    |--------|--------|--------|--------|--------|
    |    project_id    |    项目id    |    string   |    1548233985051132    |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    all_order_count    |    门店总单量    |    string   |
    |    all_all_money    |    门店总金额    |    string   |
    |    ave_shop    |    门店客单价    |    string   |
    |    shop_list[]    |    月度商户排行榜    |    list   |
    |    └merchant_name    |    商户名称    |    string   |
    |    └order_count    |    月销量    |    int   |
    |    └proportion    |    商户单量占比    |    double   |
    |    cate_list    |    月度品类榜    |    list   |
    |    └cate_name    |    品类名称    |    string   |
    |    └cate_count    |     品类销售单量   |    string   |
    |    └cate_ave    |    品类占比    |    string   |


    #### return
    - ##### json
    > [{"all_order_count": "40007", "all_all_money": "813530.04", "ave_shop": "20.334692428824958"}, "shop_list": [{"merchant_name": "轻盒有机", "order_count": 3218, "proportion": 0.08043592371335016}], "cate_list": [{"cate_name": "沙拉", "cate_count": "7194", "cate_ave": "0.1798185317569425350563651361"}]}]
    @@@"""
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

app.register_blueprint(api, url_prefix='/')
app.register_blueprint(platform, url_prefix='/platform')

if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host="169.254.230.2", port=5001, debug=True)
