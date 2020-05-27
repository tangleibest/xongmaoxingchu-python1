import xlrd
import pymysql
# 获取网规四边形里面的饿了么美团数据
import xlwt
import math
from DBUtils.PooledDB import PooledDB

# 计算两个点距离
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
    return s * 1000


# 百度经纬度转化成高德经纬度
def bdToGaoDe(lon, lat):
    """
    百度坐标转高德坐标
    :param lon:
    :param lat:
    :return:
    """
    PI = 3.14159265358979324 * 3000.0 / 180.0
    x = lon - 0.0065
    y = lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * PI)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * PI)
    lon = z * math.cos(theta)
    lat = z * math.sin(theta)
    (math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * PI)) *math.cos(math.atan2(y, x) - 0.000003 * math.cos(x * PI))
    return [lon, lat]


# 计算三角形面积
def area_triangle(a, b, c):
    s_a = (a + b + c) / 2

    aa = abs(s_a * (s_a - a) * (s_a - b) * (s_a - c))

    total_area_a = math.sqrt(aa)

    return total_area_a


# 通过射线交点计算
def IsPtInPoly(aLon, aLat, pointList):
    '''
    :param aLon: double 经度
    :param aLat: double 纬度
    :param pointList: list [(lon, lat)...] 多边形点的顺序需根据顺时针或逆时针，不能乱
    '''

    iSum = 0
    iCount = len(pointList)

    if (iCount < 3):
        return False

    for i in range(iCount):

        pLon1 = pointList[i][0]
        pLat1 = pointList[i][1]

        if (i == iCount - 1):

            pLon2 = pointList[0][0]
            pLat2 = pointList[0][1]
        else:
            pLon2 = pointList[i + 1][0]
            pLat2 = pointList[i + 1][1]

        if ((aLat >= pLat1) and (aLat < pLat2)) or ((aLat >= pLat2) and (aLat < pLat1)):

            if (abs(pLat1 - pLat2) > 0):

                pLon = pLon1 - ((pLon1 - pLon2) * (pLat1 - aLat)) / (pLat1 - pLat2);

                if (pLon < aLon):
                    iSum += 1

    if (iSum % 2 != 0):
        return True
    else:
        return False


# 写入excel

PLATFORM = 'mt'
UPDATE_COUNT = [6, 9, 12]
for i in UPDATE_COUNT:
    pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root',
                                    passwd='xmxc1234',
                                    db='mapmarkeronline', port=62864)
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    data = xlrd.open_workbook(r"C:\Users\tl\Desktop\excel\网规扎点图-需更新商圈0519.xlsx")
    table = data.sheets()[0]
    book = xlwt.Workbook(encoding='utf-8')
    sheet1 = book.add_sheet(u'Sheet1', cell_overwrite_ok=True)
    sheet_list = []
    sheet_list.append(['id', '商圈名称', '商户', '月销量', '纬度', '经度', '电话', '一级品类', '二级品类', '客单价', '四边形面积'])
    for table_index in range(3, 14):
        row = table.row_values(table_index)
        id = row[0]
        business_area_name = row[1]
        left_up_name = row[2]
        left_up_lng = bdToGaoDe(float(row[3]), float(row[4]))[0]
        left_up_lat = bdToGaoDe(float(row[3]), float(row[4]))[1]
        right_up_name = row[5]
        right_up_lng = bdToGaoDe(float(row[6]), float(row[7]))[0]
        right_up_lat = bdToGaoDe(float(row[6]), float(row[7]))[1]
        right_lower_name = row[8]
        right_lower_lng = bdToGaoDe(float(row[9]), float(row[10]))[0]
        right_lower_lat = bdToGaoDe(float(row[9]), float(row[10]))[1]
        left_lower_name = row[11]
        left_lower_lng = bdToGaoDe(float(row[12]), float(row[13]))[0]
        left_lower_lat = bdToGaoDe(float(row[12]), float(row[13]))[1]
        # 计算边长和对角线的长度，单位米
        ad = getDistance(left_lower_lat, left_lower_lng, left_up_lat, left_up_lng)
        ab = getDistance(left_up_lat, left_up_lng, right_up_lat, right_lower_lng)
        bc = getDistance(right_up_lat, right_up_lng, right_lower_lat, right_lower_lng)
        cd = getDistance(right_lower_lat, right_lower_lng, left_lower_lat, left_lower_lng)
        ac = getDistance(left_up_lat, left_up_lng, right_lower_lat, right_lower_lng)
        bd = getDistance(right_up_lat, right_up_lng, left_lower_lat, left_lower_lng)
        # 计算四边形以对角线AC分割的两个三角形面积
        area_a_AC = area_triangle(ad, cd, ac)
        area_b_AC = area_triangle(ab, bc, ac)
        # 两个三角形组组成的四边形的面积
        total_area_AC = area_a_AC + area_b_AC

        # 计算四边形以对角线BD分割的两个三角形面积
        area_a_BD = area_triangle(ab, ad, bd)
        area_b_BD = area_triangle(bc, cd, bd)
        # 两个三角形组组成的四边形的面积
        total_area_BD = area_a_BD + area_b_BD
        avg_total_area = (total_area_AC + total_area_BD) / 2
        print(total_area_AC, total_area_BD, avg_total_area)
        pointList = [(left_up_lng, left_up_lat), (right_up_lng, right_up_lat), (right_lower_lng, right_lower_lat),
                     (left_lower_lng, left_lower_lat)]
        if PLATFORM == 'mt':
            # 美团
            sql = "SELECT client_name,month_sale_num,latitude,longitude,call_center,first_cate_name,second_cate_name,average_price from t_map_client_mt_beijing_mark where " \
                  "update_count=%s and own_set_cate not in ('其他品类','超市便利')" % i
            print(business_area_name)

        elif PLATFORM == 'elm':
            # 饿了么
            sql = "SELECT client_name,month_sale_num,latitude,longitude,call_center,first_cate,second_cate from t_map_client_elm_beijing_mark where update_count=%s and own_set_cate not in ('其他品类','超市便利') " % i

            print(business_area_name)
        cur.execute(sql)
        results = cur.fetchall()

        for result in results:
            shop_lat = float(result[2])
            shop_lng = float(result[3])

            is_in = IsPtInPoly(shop_lng, shop_lat, pointList)
            if is_in is True:
                if PLATFORM == 'mt':
                    sheet_list.append(
                        [id, business_area_name, result[0], result[1], result[2], result[3], result[4], result[5],
                         result[6],
                         result[7], avg_total_area
                         ])
                elif PLATFORM == 'elm':
                    sheet_list.append(
                        [id, business_area_name, result[0], result[1], result[2], result[3], result[4], result[5],
                         result[6], avg_total_area
                         ])

    for row, line in enumerate(sheet_list):
        for col, t in enumerate(line):
            sheet1.write(row, col, t)

    book.save('%s_beijing_poly_%s.xlsx' % (PLATFORM, i))

    db.close()
    pool_mapmarkeronline.close()
