# 计算三角形面积
import math

# def area_triangle(a, b, c):
#     s_a = (a + b + c) / 2
#     print("s_a:%s" % s_a)
#     aa = abs(s_a * (s_a - a) * (s_a - b) * (s_a - c))
#     print("aa:%s" % aa)
#     total_area_a = math.sqrt(aa)
#     print("total_area_a:%s" % total_area_a)
#     return total_area_a
#
#
# a_1 = area_triangle(4, 4, 5.6568542)
# a_2 = area_triangle(4, 4, 5.6568542)
# a_3 = area_triangle(4, 2.8284, 2.8284)
# total_a = a_1 + a_2
# total_b = a_3*4
# if total_b<=total_a:
#     print(total_a,total_b)
#     print("ok")

# def multiply(p0, p1, p2, p3, p4):
#     a =(p1[])
#     return result
#
#
# # 判断点是否在四边形内部，在四边形内部返回1，否则返回0
# # x、y为点的坐标，xv为四边形的x坐标构成的向量，yv为四边形的y坐标构成的向量
# def inpolygon(x, y, xv, yv):
#     z1 = (xv[1] - xv[0]) * (y - yv[0]) - (x - xv[0]) * (yv[1] - yv[0])
#     z2 = (xv[2] - xv[1]) * (y - yv[1]) - (x - xv[1]) * (yv[2] - yv[1])
#     z3 = (xv[3] - xv[2]) * (y - yv[2]) - (x - xv[2]) * (yv[3] - yv[2])
#     z4 = (xv[0] - xv[3]) * (y - yv[3]) - (x - xv[3]) * (yv[0] - yv[3])
#     if (z1 * z2 >= 0 and z3 * z4 >= 0 and z1 * z3 >= 0):  # z1, z2, z3, z4同号
#         return 1
#     return 0
#
#
# ab = multiply(2, 2, 1, 3, 3, 3)
# ac = multiply(2, 2, 1, 3, 1, 1)
# re = inpolygon(1, 33, [1, 1, 3, 3], [1, 3, 3, 1])
# print(re)

# def IsPtInPoly(aLon, aLat, pointList):
#     '''
#     :param aLon: double 经度
#     :param aLat: double 纬度
#     :param pointList: list [(lon, lat)...] 多边形点的顺序需根据顺时针或逆时针，不能乱
#     '''
#
#     iSum = 0
#     iCount = len(pointList)
#
#     if (iCount < 3):
#         return False
#
#     for i in range(iCount):
#
#         pLon1 = pointList[i][0]
#         pLat1 = pointList[i][1]
#
#         if (i == iCount - 1):
#
#             pLon2 = pointList[0][0]
#             pLat2 = pointList[0][1]
#         else:
#             pLon2 = pointList[i + 1][0]
#             pLat2 = pointList[i + 1][1]
#
#         if ((aLat >= pLat1) and (aLat < pLat2)) or ((aLat >= pLat2) and (aLat < pLat1)):
#
#             if (abs(pLat1 - pLat2) > 0):
#
#                 pLon = pLon1 - ((pLon1 - pLon2) * (pLat1 - aLat)) / (pLat1 - pLat2);
#
#                 if (pLon < aLon):
#                     iSum += 1
#
#     if (iSum % 2 != 0):
#         return True
#     else:
#         return False
#
#
# re = IsPtInPoly(39.955578, 116.459812,
#                 [(116.451133, 39.955439), (116.476079, 39.977054), (116.496704, 39.95993), (116.468358, 39.953102)]
#                 )


# 计算三角形面积
import xlrd
import xlwt


def area_triangle(a, b, c):
    s_a = (a + b + c) / 2

    aa = abs(s_a * (s_a - a) * (s_a - b) * (s_a - c))

    total_area_a = math.sqrt(aa)
    print(a, b, c, total_area_a)
    return total_area_a


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
    return lon, lat


ab = getDistance(40.348430822382916, 116.340301, 40.34843022354403, 116.35207148802516)
bc = getDistance(40.34843022354403, 116.35207148802516, 40.33942456129158, 116.35206992291809)
cd = getDistance(40.33942456129158, 116.35206992291809, 40.33942516, 116.340301)
da = getDistance(40.33942516, 116.340301, 40.348430822382916, 116.340301)
ac = getDistance(40.348430822382916, 116.340301, 40.33942456129158, 116.35206992291809)
bd = getDistance(40.34843022354403, 116.3520714880251, 40.33942516, 116.340301)

area_a = area_triangle(ab, bc, ac)
area_b = area_triangle(da, cd, ac)
print(ab, bc, cd, da, ac, bd, area_a, area_b)
print(area_a + area_b)
print(bdToGaoDe(116.35207148802516, 40.34843022354403)[1])

book = xlwt.Workbook(encoding='utf-8')
sheet1 = book.add_sheet(u'Sheet1', cell_overwrite_ok=True)
data = xlrd.open_workbook(r"C:\Users\tl\Desktop\excel\网规扎点图钉钉5.14.xlsx")
table = data.sheets()[0]
sheet_list = []
for table_index in range(3, 92):
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
    sheet_list.append(['[', left_up_lng, ',', left_up_lat, '],[', right_up_lng, ',', right_up_lat, '],[',
                       right_lower_lng, ',', right_lower_lat, '],[',
                       left_lower_lng, ',', left_lower_lat, ']'])

for row, line in enumerate(sheet_list):
    for col, t in enumerate(line):
        sheet1.write(row, col, t)

book.save('lat_gaode.xlsx')
