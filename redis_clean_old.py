import utils
import redis
import re
import json
from datetime import datetime

r = redis.StrictRedis(host='ts-test.sfvwpz.0001.use1.cache.amazonaws.com', port=6379, db=0)

start_time = datetime.strptime("2-10-2018", '%d-%m-%Y')
count = 0

for key in r.keys('thumbs*'):
    count += 1
    if count % 1000 == 0:
        print(count)
    data = r.get(key)
    d = json.loads(data)
    if "media_url_news_thumb" in d:
        thumb_url = d["media_url_news_thumb"]

    regex_string = '/shared_data/images(.*)/'
    x = re.search(regex_string, thumb_url)

    if x:
        d = x.group(1)
        datetime_object = datetime.strptime(d, '%d-%m-%Y')
        if datetime_object < start_time:
            r.delete(key)
            # print("deleted key " + str(thumb_url))
        # else:
            # print(datetime_object)
