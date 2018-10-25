from bs4 import BeautifulSoup
import requests
import sys
import concurrent.futures
import time

def process_url_list(urls, handler_fn, result_obj={}):
    sys.setrecursionlimit(1000)

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/604.5.6 (KHTML, like Gecko) Version/11.0.3 Safari/604.5.6'}
    
    def load_url(url):
        # r = requests.get(url, headers=headers, allow_redirects=True, timeout=3)
        r = requests.get(url, allow_redirects=True, timeout=5)
        return r

    start = time.time()
    # Using 7 workers was the sweet spot on the low-RAM host I used, but you should tweak this
    # Was 10
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
    # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(load_url, link): link for link in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                handler_fn(data, url, result_obj)
                data.close()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
    runtime = time.time() - start
    sys.stdout.write("\ntook {} seconds or {} links per second".format(runtime, len(urls)/runtime) + "\n")