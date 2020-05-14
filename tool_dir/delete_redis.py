
import redis
r = redis.Redis(host='139.199.112.205', port=6379,db=1, password='xmxc1234',decode_responses=True)
list_keys = r.keys("*getTwoShopCompare*")

for key in list_keys:
    r.delete(key)