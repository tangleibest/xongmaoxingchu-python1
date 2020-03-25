import requests
import time
import hashlib
import json
from DBUtils.PooledDB import PooledDB
import pymysql
developer_id = 100002
shop_id = 5168
timestamp = int(time.time())


pool = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234', db='mapmarktest',
                port=62864)  # 5为连接池里的最少连接数
str_sig = "AhAZaQNkEBLaZSaOdeveloper_id=100002shop_id=5168timestamp=%sAhAZaQNkEBLaZSaO" % timestamp
b = str_sig.encode(encoding='utf-8')

m = hashlib.md5()
m.update(b)
md5ed = m.hexdigest()
print(md5ed)
uppered = str(md5ed).upper()
signature = uppered
print(signature)
url = "https://xmxc.wdd88.com/open/shop/get_plats"
data = {"developer_id": developer_id, "shop_id": shop_id, "timestamp": timestamp, "signature": "%s" % signature}
res = requests.post(url, json.dumps(data))
print(res.text)
