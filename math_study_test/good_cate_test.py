import math
import pandas as pd

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

pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                                db='mapmarkeronline', port=62864)
sql="SELECT latitude,longitude,own_set_cate,month_sale_num,client_name FROM t_map_client_mt_beijing_mark WHERE own_set_cate='夹馍饼类'and update_count=10 AND own_first_cate NOT IN ('商店超市','果蔬生鲜','鲜花绿植','医药健康')"
db = pool_mapmarkeronline.connection()
cur = db.cursor()
cur.execute(sql)
results = cur.fetchall()
key_value = []
data_value = []
month_sale_list = {}

lat=39.989440
lng=116.482756

for row in results:
    latitude = row[0]
    longitude = row[1]
    dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
    if dis <= 3:
        print(row[4],row[3])
        key_value.append(row[2])
        data_value.append(int(row[3]))
month_sale_list['key'] = key_value
month_sale_list['data'] = data_value
df = pd.DataFrame(month_sale_list)
kk = df.groupby(['key'], as_index=False)['data'].sum()
kk_count = df.groupby(['key'], as_index=False)['data'].count()
kk_values=kk.values
kk_count_values=kk_count.values
a=sorted(kk_values,key=(lambda x:x[0]))
# for i in kk_values:
#     print(i[0],i[1])
# print('-------------------------------------------------')
# for i in kk_count_values:
#     print(i[0],i[1])
