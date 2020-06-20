import math

import pymysql

"""
计算ka品牌商户
"""

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
    return s * 1000


# 配置
# 饿了么更新次数
UPDATE_COUNT_ELM = 14
# 美团更新次数
UPDATE_COUNT_MT = 12
# 大于几家算品牌
CATE_SHOP_COUNT = 10
# 城市
CITY_LIST = ['北京市', '上海市', '杭州市']
# 平台
PLATFORM_ELM = 'elm'
PLATFORM_MT = 'mt'
db = pymysql.connect(host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                     db='mapmarkeronline', port=62864)
cursor = db.cursor()
for CITY in CITY_LIST:
    if CITY == '北京市':
        city_name = 'beijing'
        city_id = 1
    elif CITY == '上海市':
        city_name = 'shanghai'
        city_id = 2
    elif CITY == '杭州市':
        city_name = 'hangzhou'
        city_id = 3

    # 饿了么品牌
    cursor.execute("SELECT SUBSTRING_INDEX(replace(client_name,'（','('),'(',1),COUNT(*)  c,own_set_cate FROM "
                   "t_map_client_%s_%s_mark where update_count =%s and own_set_cate not in ('超市便利') "
                   "GROUP BY SUBSTRING_INDEX(replace(client_name,'（','('),'(',1)  HAVING c >=%s ORDER BY c desc" % (
                       PLATFORM_ELM, city_name, UPDATE_COUNT_ELM, CATE_SHOP_COUNT))
    ka_elm_data = cursor.fetchall()
    # 美团品牌
    cursor.execute("SELECT SUBSTRING_INDEX(replace(client_name,'（','('),'(',1),COUNT(*)  c,own_set_cate FROM "
                   "t_map_client_%s_%s_mark where update_count =%s and own_set_cate not in ('超市便利') "
                   "GROUP BY SUBSTRING_INDEX(replace(client_name,'（','('),'(',1)  HAVING c >=%s ORDER BY c desc" % (
                       PLATFORM_MT, city_name, UPDATE_COUNT_MT, CATE_SHOP_COUNT))
    ka_mt_data = cursor.fetchall()

    # 对饿了么和美团计算得到的品牌合并去重
    ka_dict = {}
    for row_ka_elm in ka_elm_data:
        ka_dict[str(row_ka_elm[0]).lower()] = row_ka_elm[2]
    for row_ka_mt in ka_mt_data:
        ka_dict[str(row_ka_mt[0]).lower()] = row_ka_mt[2]

    # 饿了么店铺
    cursor.execute("SELECT "
                   "client_name,month_sale_num,address,latitude,longitude,own_set_cate,call_center,shipping_time,rating "
                   "from "
                   "t_map_client_%s_%s_mark "
                   "where "
                   "update_count=%s and "
                   "own_set_cate not in ('超市便利') and  month_sale_num>0 " % (
                       PLATFORM_ELM, city_name, UPDATE_COUNT_ELM))
    shop_elm_data = cursor.fetchall()

    # 美团店铺
    cursor.execute("SELECT "
                   "client_name,month_sale_num,address,latitude,longitude,own_set_cate,call_center,average_price "
                   " ,shipping_time,rating from "
                   "t_map_client_%s_%s_mark "
                   "where "
                   "update_count=%s and "
                   "own_set_cate not in ('超市便利') and  month_sale_num>0" % (
                       PLATFORM_MT, city_name, UPDATE_COUNT_MT))
    shop_mt_data = cursor.fetchall()

    # 计算品牌下商户
    ka_month_sale_dict = {}
    ka_month_sale_dict2 = {}
    # 饿了么商户
    for shop_elm in shop_elm_data:
        client_name = shop_elm[0]
        client_name_replace = str(client_name).replace("（", "(").replace("）", ")")
        address = str(shop_elm[2]).replace("（", "(").replace("）", ")")
        # 分割得到(前面的品牌名称，并去除空格，并把英文转化成小写
        client_name_split = str(client_name_replace).split("(")[0].strip(" ").lower()
        # 匹配
        if client_name_split in ka_dict.keys():
            if '(' in client_name_replace:
                ka_month_sale_dict[client_name_replace] = [city_id, CITY, shop_elm[4], shop_elm[3], client_name_replace,
                                                           address, shop_elm[6], client_name_split, 0, shop_elm[1],
                                                           shop_elm[5], None, shop_elm[7], shop_elm[8]]
            else:
                ka_month_sale_dict[client_name_replace + address] = [city_id, CITY, shop_elm[4], shop_elm[3],
                                                                     client_name_replace,
                                                                     address, shop_elm[6], client_name_split, 0,
                                                                     shop_elm[1],
                                                                     shop_elm[5], None, shop_elm[7], shop_elm[8]]

    # 美团商户
    for shop_mt in shop_mt_data:
        client_name = shop_mt[0]
        mt_month_sale = shop_mt[1]
        mt_month_ave_sale = shop_mt[7]
        client_name_replace = str(client_name).replace("（", "(").replace("）", ")")

        address = str(shop_mt[2]).replace("（", "(").replace("）", ")")
        client_name_split = str(client_name_replace).split("(")[0].strip(" ").lower()
        # 如果这个店是品牌店做处理
        if client_name_split in ka_dict.keys():
            # 如果美团这个店铺在ka_month_sale_dict字典里面，进行处理
            if client_name_replace in ka_month_sale_dict.keys():
                # 如果店名包含（则直接匹配上，然后更新结果列表的美团单量
                if '(' in client_name_replace:
                    ka_month_sale_dict[client_name_replace][8] = shop_mt[1]
                    ka_month_sale_dict[client_name_replace][11] = shop_mt[7]
                #     如果店名不包含（，则遍历出该ka饿了么的所有门店，通过计算距离小于120米认为是一家门店
                else:
                    dis_list = []
                    for ka_key in ka_month_sale_dict:
                        ka_value = ka_month_sale_dict[ka_key]
                        ka_name = ka_value[7]
                        if ka_name == client_name_split:
                            dis = getDistance(float(shop_mt[3]), float(shop_mt[4]), float(ka_value[3]),
                                              float(ka_value[2]))
                            if dis <= 180:
                                dis_list.append([dis, ka_key])
                    dis_list = sorted(dis_list, key=(lambda x: x[0]))
                    ka_month_sale_dict[dis_list[0][1]][8] = shop_mt[1]
                    ka_month_sale_dict[dis_list[0][1]][11] = shop_mt[7]

                    # ka_month_sale_dict.get(client_name_replace + address)[8] = shop_mt[1]
                    # ka_month_sale_dict.get(client_name_replace)[9] = shop_mt[6]
            # 如果美团店名和饿了么不一致
            else:
                dis_list2 = []
                for ka_key2 in ka_month_sale_dict:
                    ka_value2 = ka_month_sale_dict[ka_key2]
                    ka_name2 = ka_value2[7]
                    # 如果美团的品牌名和client_name_split字典的品牌名一样做处理
                    if ka_name2 == client_name_split:
                        dis = getDistance(float(shop_mt[3]), float(shop_mt[4]), float(ka_value2[3]),
                                          float(ka_value2[2]))
                        # 如果美团的这个店和client_name_split中的店小于120米就更新这个店的数据
                        if dis <= 180:
                            dis_list2.append([dis, ka_key2])
                        # 如果大于120就新增到client_name_split字典中
                        else:
                            if '(' in client_name_replace:
                                ka_month_sale_dict2[client_name_replace] = [city_id, CITY, shop_mt[4], shop_mt[3],
                                                                            client_name_replace,
                                                                            address, shop_mt[6], client_name_split,
                                                                            shop_mt[1], 0,
                                                                            shop_mt[5], shop_mt[7], shop_mt[8],
                                                                            shop_mt[9]]
                            else:
                                ka_month_sale_dict2[client_name_replace + address] = [city_id, CITY, shop_mt[4],
                                                                                      shop_mt[3],
                                                                                      client_name_replace,
                                                                                      address, shop_mt[6],
                                                                                      client_name_split,
                                                                                      shop_mt[1], 0,
                                                                                      shop_mt[5], shop_mt[7],
                                                                                      shop_mt[8], shop_mt[9]]
                if len(dis_list2) > 0:
                    dis_list2 = sorted(dis_list2, key=(lambda x: x[0]))
                    ka_month_sale_dict[dis_list2[0][1]][8] = shop_mt[1]
                    ka_month_sale_dict[dis_list2[0][1]][11] = shop_mt[7]
    # for a in ka_month_sale_dict.keys():
    #     print(a, ka_month_sale_dict.get(a))
    # 写入数据库
    ka_month_sale_dict3 = {**ka_month_sale_dict, **ka_month_sale_dict2}
    insert_sql = "insert into " \
                 "t_map_ka_mark_bak" \
                 "(city_id,city_name,longitude,latitude,mark_name,mark_address,mark_tel,ka_name,mark_sale_num_mt," \
                 "mark_sale_num_elm,ka_cate_name,mark_sale_ave_mt,,opening_hours,rating) " \
                 "values " \
                 "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    for row in ka_month_sale_dict3.keys():
        list_insert = ka_month_sale_dict3.get(row)

        insert_data = tuple(list_insert)

        cursor.execute(insert_sql, insert_data)
    db.commit()
cursor.close()
db.close()
