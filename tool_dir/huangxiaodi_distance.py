import xlrd
import csv
import requests
import json
import time
import math

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


data = xlrd.open_workbook('C:\\Users\\tl\\Documents\\WeChat Files\\t1220149242\\FileStorage\\File\\2019-12\\黄小递地址.xlsx')
data2 = xlrd.open_workbook(
    'C:\\Users\\tl\\Documents\\WeChat Files\\t1220149242\\FileStorage\\File\\2019-10\\全国地址(2).xlsx')
# 1. 获取excel sheet对象
table1 = data.sheets()[0]

table2 = data2.sheets()[0]

f = open('reslutes.csv', 'w', newline="", encoding='utf-8')
csv_writer = csv.writer(f)

for i in range(1, 24):
    row = table1.row_values(i)
    location_58 = str(row[3])
    print(location_58)
    lat_58 = str(location_58).split(",")[1]
    lng_58 = str(location_58).split(",")[0]
    project = []
    for a in range(1, 107):
        row2 = table2.row_values(a)
        location_xmxc = row2[4]
        lat_xmxc = str(location_xmxc).split(",")[1]
        lng_xmxc = str(location_xmxc).split(",")[0]
        distance = getDistance(float(lat_58), float(lng_58), float(lat_xmxc), float(lng_xmxc))
        if distance <= 5:
            # project.append([row2[2],distance])

            csv_writer.writerow([row[0], row[1], row[2], row[3], distance, row2[2]])
f.close()
