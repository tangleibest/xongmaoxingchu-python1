import datetime

import requests
import time
import hashlib
import json
from retrying import retry
from DBUtils.PooledDB import PooledDB
import pymysql

start_time = datetime.datetime.today()
pool = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234', db='mapmarktest',
                port=62864)  # 5为连接池里的最少连接数
db = pool.connection()
cur = db.cursor()


def get_str(str_name):
    if str_name is None or str_name == "None":
        str_name = ''
    return str_name


def get_signature(page, page_count, timestamp, shop_id, date):
    str_sig = "AhAZaQNkEBLaZSaOdate=\"%s\"developer_id=100002page=%spage_count=%sshop_id=%stimestamp=%sAhAZaQNkEBLaZSaO" % (
        date, page, page_count, shop_id, timestamp)
    b = str_sig.encode(encoding='utf-8')

    m = hashlib.md5()
    m.update(b)
    md5ed = m.hexdigest()

    uppered = str(md5ed).upper()
    signature = uppered
    return signature


@retry(stop_max_attempt_number=5, wait_random_min=5000, wait_random_max=10000)
def get_page(shop_id, date):
    developer_id = 100002
    timestamp = int(time.time())
    sin_str = "AhAZaQNkEBLaZSaO"
    page = 1
    page_count = 20
    # signature=

    signature = get_signature(1, 20, timestamp, shop_id, date)
    # 获得页数

    url = "https://xmxc.wdd88.com/open/order/list"
    data = {"date": date, "developer_id": developer_id, "page": page, "page_count": page_count, "shop_id": shop_id,
            "timestamp": timestamp,
            "signature": "%s" % signature}
    res = requests.post(url, json.dumps(data)).text

    res = json.loads(res)
    code = res.get("code")
    page_count_get=0
    if code == 0:
        pager = res.get("pager")
        page_count_get = pager.get("pageCount")
    return page_count_get


@retry(stop_max_attempt_number=5, wait_random_min=5000, wait_random_max=10000)
def get_data(page_count, shop_id, date):
    # print("重试")
    url = "https://xmxc.wdd88.com/open/order/list"
    developer_id = 100002

    timestamp2 = int(time.time())
    page_signature = get_signature(page_count, 20, timestamp2, shop_id, date)
    page_data = {"date": date, "developer_id": developer_id, "page": page_count, "page_count": 20, "shop_id": shop_id,
                 "timestamp": timestamp2,
                 "signature": "%s" % page_signature}
    page_res = requests.post(url, json.dumps(page_data)).text

    datas = json.loads(page_res).get("datas")

    order_list = []
    for row in datas:
        shop = datas.get(str(row))
        data_id = shop.get("id")
        client_id = shop.get("client_id")

        status = shop.get("status")
        send_status = shop.get("send_status")
        pay_type = shop.get("pay_type")
        pay_status = shop.get("pay_status")
        source = shop.get("source")
        service_type = shop.get("service_type")
        plat_num = shop.get("plat_num")
        order_no = shop.get("order_no")
        desk_code = shop.get("desk_code")
        buyer_name = shop.get("buyer_name")
        buyer_phone = shop.get("buyer_phone")
        buyer_address = shop.get("buyer_address")
        buyer_distance = shop.get("buyer_distance")
        buyer_lat = shop.get("buyer_lat")
        buyer_lat = get_str(buyer_lat)
        buyer_lng = shop.get("buyer_lng")
        buyer_lng = get_str(buyer_lng)
        send_uid = shop.get("send_uid")
        send_uid = get_str(send_uid)
        send_name = shop.get("send_name")
        send_name = get_str(send_name)
        send_mobile = shop.get("send_mobile")
        send_mobile = get_str(send_mobile)
        required = shop.get("required")
        required = get_str(required)
        is_send_appoint = shop.get("is_send_appoint")
        good_time = shop.get("good_time")
        created_at = shop.get("created_at")
        origin_amount = shop.get("origin_amount")
        box_amount = shop.get("box_amount")
        send_amount = shop.get("send_amount")
        order_amount = shop.get("order_amount")
        subsidy_amount = shop.get("subsidy_amount")
        discount_amount = shop.get("discount_amount")
        service_amount = shop.get("service_amount")
        red_amount = shop.get("red_amount")
        income_amount = shop.get("income_amount")
        is_invoice = shop.get("is_invoice")
        invoice_name = shop.get("invoice_name")
        is_print = shop.get("is_print")
        send_appointed_at = shop.get("send_appointed_at")
        send_started_at = shop.get("send_started_at")
        send_arrived_at = shop.get("send_arrived_at")
        products_list = shop.get("products")
        products_inster = []
        # for products in products_list:
        #     products_id = products.get("id")
        #     order_id = products.get("order_id")
        #     product_id = products.get("product_id")
        #     product_extend_id = products.get("product_extend_id")
        #     origin_price = products.get("origin_price")
        #     buy_price = products.get("buy_price")
        #     buy_num = products.get("buy_num")
        #     is_product = products.get("is_product")
        #     product_name = products.get("product_name")
        #     spec = products.get("spec")
        #     spec = get_str(spec)
        #     code = products.get("code")
            # products_inster.append(
            #     [client_id, order_id, products_id, product_name, buy_num, buy_price, spec, date, origin_price,
            #      is_product])
        # for row_ins in products_inster:
        #     sql2 = "INSERT IGNORE into t_map_client_wmb_products_2019_2 VALUES (%s,%s, %s,\"%s\",%s,%s,\"%s\",\"%s\",%s,\"%s\")" % (
        #         row_ins[0], row_ins[1], row_ins[2], row_ins[3], row_ins[4], row_ins[5],
        #         row_ins[6], row_ins[7], row_ins[8], row_ins[9])
        #     cur.execute(sql2)  # 执行sql语句
        #     db.commit()  # 提交到数据库执行
        order_list.append(
            [client_id, data_id, buyer_name, buyer_phone, buyer_address, buyer_lat, buyer_lng, required, income_amount,
             source, buyer_distance, date, send_status, pay_type, pay_status, service_type, plat_num, order_no,
             send_uid, send_name, send_mobile, is_send_appoint, good_time, created_at, origin_amount, box_amount,
             send_amount, order_amount, subsidy_amount, discount_amount, service_amount, red_amount, is_print,
             send_appointed_at, send_started_at, send_arrived_at, status])
    for inster_row in order_list:
        insert_sql = "INSERT IGNORE into t_map_client_wmb_user_2019_2 VALUES (%s,%s," \
                     "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,\"%s\",%s,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"," \
                     "%s,\"%s\",\"%s\",\"%s\",\"%s\",%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")" % \
                     (inster_row[0], inster_row[1], inster_row[2], inster_row[3], inster_row[4], inster_row[5],
                      inster_row[6], inster_row[7], inster_row[8], inster_row[9], inster_row[10], inster_row[11],
                      inster_row[12], inster_row[13], inster_row[14], inster_row[15], inster_row[16], inster_row[17],
                      inster_row[18],
                      inster_row[19], inster_row[20], inster_row[21], inster_row[22], inster_row[23], inster_row[24],
                      inster_row[25], inster_row[26], inster_row[27], inster_row[28], inster_row[29], inster_row[30],
                      inster_row[31],
                      inster_row[32], inster_row[33], inster_row[34], inster_row[35], inster_row[36])

        cur.execute(insert_sql)  # 执行sql语句
        db.commit()  # 提交到数据库执行


shop_sql = "SELECT shop_id from t_map_client_wmb_shop where shop_id in (5053,5054,5058,5059)"
cur.execute(shop_sql)
results = cur.fetchall()
date = str(datetime.date.today() - datetime.timedelta(days=1))

for shop_id_list in results:

    shop_id = shop_id_list[0]
    print(shop_id)

    page_count_get = get_page(shop_id, date)
    if page_count_get > 0:
        for page_count in range(1, page_count_get + 1):
            get_data(page_count, shop_id, date)
db.close()
end_time = datetime.datetime.today()
print("新增成功开始时间：%s,结束时间：%s" % (start_time, end_time))
