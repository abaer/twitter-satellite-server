import utils
import redis
import json

r = redis.StrictRedis(host='ts-test.sfvwpz.0001.use1.cache.amazonaws.com', port=6379, db=0)


count = 0

for key in r.keys('*'):
    count += 1
    if count % 1000 == 0:
        print(count)
    if r.ttl(key) == -1:
        r.expire(key, 60 * 60 * 24 * 3)
        # This would clear them out in a week

