import pymysql
import xlwt
from DBUtils.PooledDB import PooledDB

pool = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                db='test', port=62864)

db = pool.connection()
cur = db.cursor()

pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                                db='mapmarkeronline', port=62864)

db_mapmarkeronline = pool_mapmarkeronline.connection()
cur_mapmarkeronline = db_mapmarkeronline.cursor()

sql_lat = "SELECT * from t_map_lat_lng where length=1000"
cur.execute(sql_lat)
results_lat = cur.fetchall()

sql_mark = "SELECT latitude,longitude,month_sale_num from t_map_client_elm_beijing_mark where update_count=14  union all  SELECT latitude,longitude,month_sale_num from t_map_client_mt_beijing_mark where update_count=13 "
cur_mapmarkeronline.execute(sql_mark)
results_mark = cur_mapmarkeronline.fetchall()

sql_buildings = "SELECT '商场',buildings_latitude,buildings_longitude from t_map_buildings where city_name='北京' and buildings_latitude !='不明'  " \
                "UNION ALL SELECT '小区',latitude,longitude from t_map_lianjia_uptown where city='北京市' and latitude !='不明' " \
                "UNION all  SELECT '写字楼',latitude,longitude from t_map_office_building where b_city='北京市' and latitude !='不明' " \
                "union all  SELECT '学校',school_lat,school_lng from t_map_school_info where school_city='北京市' and school_lat !='不明' " \
                "union all SELECT '医院',hospital_lat,hospital_lng from t_map_hospital_info where hospital_city='北京市' and hospital_lat !='不明'"
cur_mapmarkeronline.execute(sql_buildings)
results_buildings = cur_mapmarkeronline.fetchall()

book = xlwt.Workbook(encoding='utf-8')
sheet1 = book.add_sheet(u'Sheet1', cell_overwrite_ok=True)
sheet_list = []
for row_lat in results_lat:
    lump_id = row_lat[0]
    left_lower = row_lat[1]
    left_lower_lng = float(str(left_lower).split(',')[0])
    left_lower_lat = float(str(left_lower).split(',')[1])
    left_up = row_lat[2]
    left_up_lng = float(str(left_up).split(',')[0])
    left_up_lat = float(str(left_up).split(',')[1])
    right_lower = row_lat[3]
    right_lower_lng = float(str(right_lower).split(',')[0])
    right_lower_lat = float(str(right_lower).split(',')[1])
    right_up = row_lat[4]
    right_up_lng = float(str(right_up).split(',')[0])
    right_up_lat = float(str(right_up).split(',')[1])
    month_sale = 0
    buildings_count = 0
    for row_mark in results_mark:

        mark_lat = float(row_mark[0])
        mark_lng = float(row_mark[1])
        if mark_lat >= left_lower_lat and mark_lat < left_up_lat and mark_lng >= left_lower_lng and mark_lng < right_lower_lng:

            month_sale += row_mark[2]
    for row_buildings in results_buildings:
        building_lat = float(row_buildings[1])
        building_lng = float(row_buildings[2])
        if building_lat >= left_lower_lat and building_lat < left_up_lat and building_lng >= left_lower_lng and building_lng < right_lower_lng:
            buildings_count += 1
    sheet_list.append([lump_id, left_lower, left_up, right_lower, right_up, month_sale, buildings_count])

for row, line in enumerate(sheet_list):
    for col, t in enumerate(line):
        sheet1.write(row, col, t)
book.save('lump_level_result.xls')

db.close()
db_mapmarkeronline.close()
