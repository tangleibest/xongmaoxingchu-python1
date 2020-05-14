import requests
import time
import hashlib
import json
from retrying import retry
from DBUtils.PooledDB import PooledDB
import pymysql

pool = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234', db='mapmarktest',
                port=62864)  # 5为连接池里的最少连接数
db = pool.connection()
cur = db.cursor()


def get_str(str_name):
    if str_name is None or str_name == "None":
        str_name = ''
    return str_name


def get_signature(page, page_count, timestamp):
    str_sig = "AhAZaQNkEBLaZSaOdeveloper_id=100002page=%spage_count=%stimestamp=%sAhAZaQNkEBLaZSaO" % (
        page, page_count, timestamp)
    b = str_sig.encode(encoding='utf-8')

    m = hashlib.md5()
    m.update(b)
    md5ed = m.hexdigest()

    uppered = str(md5ed).upper()
    signature = uppered
    return signature


@retry(stop_max_attempt_number=10, wait_random_min=5000, wait_random_max=10000)
def get_page():
    developer_id = 100002
    timestamp = int(time.time())
    sin_str = "AhAZaQNkEBLaZSaO"
    page = 1
    page_count = 20
    # signature=

    signature = get_signature(1, 20, timestamp)
    # 获得页数

    url = "https://xmxc.wdd88.com/open/shop/get_shops"
    data = {"developer_id": developer_id, "page": page, "page_count": page_count, "timestamp": timestamp,
            "signature": "%s" % signature}
    res = requests.post(url, json.dumps(data)).text

    pager = json.loads(res).get("pager")
    page_count_get = pager.get("pageCount")
    return page_count_get


@retry(stop_max_attempt_number=10, wait_random_min=5000, wait_random_max=10000)
def get_data(page_count):
    url = "https://xmxc.wdd88.com/open/shop/get_shops"
    developer_id = 100002

    timestamp2 = int(time.time())
    page_signature = get_signature(page_count, 20, timestamp2)
    page_data = {"developer_id": developer_id, "page": page_count, "page_count": 20, "timestamp": timestamp2,
                 "signature": "%s" % page_signature}
    page_res = requests.post(url, json.dumps(page_data)).text
    shops = json.loads(page_res).get("shops")
    shop_list = []
    for row in shops:
        shop = shops.get(str(row))

        shop_id = shop.get("id")
        client_name = shop.get("client_name")
        client_name = get_str(client_name)
        client_code = shop.get("client_code")
        client_code = get_str(client_code)
        address = shop.get("address")
        address = get_str(address)
        map_address = shop.get("map_address")
        map_address = get_str(map_address)
        map_lng = shop.get("map_lng")
        map_lng = get_str(map_lng)
        map_lat = shop.get("map_lat")
        map_lat = get_str(map_lat)
        phone = shop.get("phone")
        phone = get_str(phone)
        brand_name = shop.get("brand_name")
        brand_name = get_str(brand_name)
        city_name = shop.get("city_name")
        city_name = get_str(city_name)
        sort_name = shop.get("sort_name")
        sort_name = get_str(sort_name)
        shop_list.append(
            [shop_id, sort_name, brand_name, client_name, client_code, address, map_address, map_lat, map_lng, phone, 0,
             '', 0, city_name])

    for inster_row in shop_list:
        insert_sql = "INSERT IGNORE into t_map_client_wmb_shop_copy VALUES (%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s',%s,'%s')" % \
                     (inster_row[0], inster_row[1], inster_row[2], inster_row[3], inster_row[4], inster_row[5],
                      inster_row[6], inster_row[7], inster_row[8], inster_row[9], inster_row[10], inster_row[11],
                      inster_row[12], inster_row[13])

        cur.execute(insert_sql)  # 执行sql语句
    db.commit()  # 提交到数据库执行


delete_sql = "DELETE from t_map_client_wmb_shop_copy"

cur.execute(delete_sql)
db.commit()
page_count_get = get_page()
for page_count in range(1, page_count_get + 1):
    get_data(page_count)
db.close()
