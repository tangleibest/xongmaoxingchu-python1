import pymysql

"""
计算品牌
"""
UPDATE_COUNT = 15
CATE_SHOP_COUNT = 10
CITY = '北京市'
PLATFORM = 'elm'
if CITY == '北京市':
    city_name = 'beijing'
elif CITY == '上海市':
    city_name = 'shanghai'
elif CITY == '杭州市':
    city_name = 'hangzhou'

db = pymysql.connect(host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                     db='mapmarkeronline', port=62864)
cursor = db.cursor()
cursor.execute("SELECT SUBSTRING_INDEX(replace(client_name,'（','('),'(',1),COUNT(*)  c,own_set_cate FROM "
               "t_map_client_%s_%s_mark where update_count =%s and own_set_cate not in ('超市便利') "
               "GROUP BY SUBSTRING_INDEX(replace(client_name,'（','('),'(',1)  HAVING c >=%s ORDER BY c desc" % (
                   PLATFORM, city_name, UPDATE_COUNT, CATE_SHOP_COUNT))
data = cursor.fetchall()

cursor.execute("SELECT ka_name,icon_address FROM t_map_ka_mark GROUP BY ka_name")
icon_data = cursor.fetchall()

icon_dict = {}
for icon_row in icon_data:
    icon_dict[icon_row[0]] = icon_row[1]

insert_sql = "insert into t_map_h5_ka_shop  (city,platform,brand_name,icon,cate_name) values (%s,%s,%s,%s,%s)"

for row in data:
    icon = icon_dict.get(row[0], '')
    insert_data = (CITY, PLATFORM, row[0], icon, row[2])

    cursor.execute(insert_sql, insert_data)
db.commit()

cursor.close()
db.close()
