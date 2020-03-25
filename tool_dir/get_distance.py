import math
from DBUtils.PooledDB import PooledDB
import pymysql

jingguodasha = '116.345804,39.96034'
chaowai = '116.442037,39.922104'
shuangjing = "116.462052,39.89248"

list1 = ['116.49424,39.976068']
# 计算两个经纬度之间的距离
EARTH_REDIUS = 6378.137


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
                                db='test', port=62864)
db = pool_mapmarkeronline.connection()
cur = db.cursor()
for row in list1:
    lng1 = row.split(',')[0]
    lat1 = row.split(',')[1]
    sql = "SELECT  title,monthSaleCount,telephones,lat,lon from new_beijing_mark where update_count=14 and  city='北京市' and createDate>=1580486400"
    cur.execute(sql)
    results = cur.fetchall()
    sum_sale = 0
    for row1 in results:
        dic = getDistance(float(lat1), float(lng1), float(row1[3]), float(row1[4]))
        if dic <= 1:
            sum_sale += 1
            print(dic, row1)
    # print(sum_sale)
db.close()
