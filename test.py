import redis
#redis连接池
pool = redis.ConnectionPool(host='139.199.112.205', port=6379,password='xmxc1234',db=1, decode_responses=True)
redis_conn = redis.Redis(connection_pool=pool)

a=redis_conn.hgetall("/api/getCateShop")
b=redis_conn.hget('/api/getCateShop','116.49432,39.97605')
for row in a:
    print(row)