import math

import pymysql
from DBUtils.PooledDB import PooledDB

EARTH_REDIUS = 6378.137


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


city_list = ['北京']
update_time = 8
update_time2 = '2019-05-01'

pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                                db='mapmarkeronline', port=62864)

pool_project = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei',
                        passwd='tanglei', db='commerce',
                        port=3306)
for city in city_list:
    if city == '北京':
        city_name = 'beijing'
    elif city == '上海':
        city_name = 'shanghai'
    elif city == '杭州':
        city_name = 'hangzhou'
    elif city == '深圳':
        city_name = 'shenzhen'
    db = pool_project.connection()
    cur = db.cursor()
    sql = "SELECT a.project_id,a.project_name,b.address,b.latitude,b.longitude from project a LEFT JOIN development.project_base_info b on a.project_id=b.tid WHERE a.project_id in (1548233987175462,1566268131111237,1548233984714727,1565597638438726,1558942470683858,1548233984304802) and a.area_name='%s'" % city
    cur.execute(sql)
    results = cur.fetchall()

    db2 = pool_mapmarkeronline.connection()

    sql2 = "select shop_id,client_name,month_sale_num,latitude,longitude,address,0,own_set_cate from t_map_client_elm_%s_mark where  own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and update_count=%s and month_sale_num!=0" % (
    city_name, update_time)
    cur2 = db2.cursor()
    cur2.execute(sql2)
    results2 = cur2.fetchall()

    sql_inset = "INSERT  into t_map_h5_shop  (project_id,cate_name,shop_id,client_name,month_sale_num,ave_price,platform,distance,latitude,longitude,address,city,update_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
    cur3 = db2.cursor()
    index = 1

    list1 = [[1548233987175462, '国贸', '116.447587', '39.906753'],
             [1548233985397507, '上地', '116.311730', '40.028740'],
             [1548233984714727, '双井', '116.465519', '39.891299'],
             [1548233983869361, '望京', '116.482756', '39.989440'],
             [1548233982272822, '右安门', '116.365739', '39.868642'],
             [1548233981747979, '中关村', '116.328217', '39.984981']
             ]
    for row in list1:
        project_id = row[0]
        project_name = row[1]
        project_latitude = float(row[3])
        project_longitude = float(row[2])
        print('计算了%s个门店了！！' % index)
        index += 1
        for row2 in results2:
            shop_id = row2[0]
            client_name = row2[1]
            month_sale_num = row2[2]
            latitude = row2[3]
            longitude = row2[4]
            address = row2[5]
            average_price = str(row2[6])
            own_set_cate = row2[7]
            distance = getDistance(project_latitude, project_longitude, float(latitude), float(longitude))
            if distance <= 3:
                values = (
                project_id, own_set_cate, shop_id, client_name, month_sale_num, average_price, 'good_elm', distance,
                latitude, longitude, address, city, update_time2)
                cur3.execute(sql_inset, values)
        db2.commit()
    cur3.close()
    db.close()
    db2.close()
