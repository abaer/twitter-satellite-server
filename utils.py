from twitter import Twitter, OAuth
from pathlib import Path
import json
import boto3
import re
from io import BytesIO
import pandas as pd
from six import string_types

## REDIS CACHE UTILS
import redis
r = redis.StrictRedis(host='ts-test.sfvwpz.0001.use1.cache.amazonaws.com', port=6379, db=0)

def add_to_cache(k, v, prefix, json_type=True, ex=None):
    key = prefix + ":" + k
    try:
        val = json.dumps(v) if json_type == True else v.encode('utf-8')
        if ex == None:
            r.set(key, val)
        else:
            r.set(key, val, ex=ex)
    except Exception as e: 
        print("problem adding to cache: ")
        print(e)
        print(k)
        print(v)
    return None

def get_from_cache(k, prefix, json_type=True, print_hits=False):
    s = r.get(prefix + ":" + k)
    val = None
    if s != None:
        val = json.loads(s) if json_type == True else s.decode('utf-8')
        if print_hits:
            print("Cache hit for " + k)
        # print("returned " + json.dumps(val)
    return val

def get_from_cache_m(keys, prefix, json_type=True, print_hits=False):
    if len(keys) == 0:
        return {}, []
    keys_prefix = [prefix + ":" + k for k in keys]
    res_m = r.mget(keys_prefix)
    if json_type == True:
        result_dict = {k:json.loads(res_m[i]) for i, k in enumerate(keys) if res_m[i] != None}
    else:
        result_dict = {k:res_m[i].decode('utf-8') for i, k in enumerate(keys) if res_m[i] != None}
    miss_list = [k for i, k in enumerate(keys) if res_m[i] == None]
    return result_dict, miss_list
## REDIS CACHE UTILS

def write_to_s3(json_payload, name, directory, public=False ):
    s3 = boto3.resource('s3')
    put_path = directory + name
    ## DEBUG CODE
    # for k,v in json_payload.items():
    #     try:
    #         print("type: " + str(type(v["domainless_title"])== str))
    #         test = json.dumps(v)
    #         test2 = json.dumps(k)

    print("Writing to " + put_path)
    try:
        resp = s3.Bucket('twitter-satellite').put_object(Key=put_path, Body=json.dumps(json_payload, skipkeys=True))
        if public == True:
            object_acl = s3.ObjectAcl('twitter-satellite',put_path)
            response = object_acl.put(ACL='public-read')
    except Exception as e: 
        print("problem writing to S3: " + name)
        print(e)
    return None;

def write_binary_to_s3(binary, name, directory, public=False ):
    s3 = boto3.resource('s3')
    put_path = directory + name
    print("Writing binary file to " + put_path)
    resp = s3.Bucket('twitter-satellite').put_object(Key=put_path, Body=binary)
    if public == True:
        object_acl = s3.ObjectAcl('twitter-satellite',put_path)
        object_acl.put(ACL='public-read')
    return resp

def read_from_s3(name, directory, seed={}):
    try:
        s3 = boto3.resource('s3')
        read_path = directory + name
        print("Reading from " + read_path)
        results = boto3.client('s3').list_objects(Bucket='twitter-satellite', Prefix=read_path)
        if 'Contents' in results:
            result = s3.Object('twitter-satellite',read_path).get()
            text = result["Body"].read().decode('utf-8')
            json_result = json.loads(text)
        else:
            json_result = seed
        return json_result
    except:
        print("Error writing binary file " + read_path)

def read_binary_from_s3(name, directory, seed={}):
    try:
        s3 = boto3.resource('s3')
        read_path = directory + name
        print("Reading binary file from " + read_path)
        result = s3.Object('twitter-satellite',read_path).get()
        r = BytesIO(result['Body'].read())
    except:
        r = seed
        print("Binary file " + read_path + " doesn't exist")
    return r

def dir_name_list(directory):
    # s3 = boto3.resource('s3')
    results = boto3.client('s3').list_objects(Bucket='twitter-satellite', Prefix=directory)
    if "Contents" in results:
        key_list = [f["Key"].split("/")[-1] for f in results["Contents"]]
    else:
        key_list = []
    return key_list

def file_date():
    date =  pd.Timestamp.now(tz='America/Los_Angeles')
    # fd = str(date.day) + "-" + str(date.year)
    fd = str(date.day) + "-" + str(date.month) + "-" +str(date.year)
    return fd

def file_name(date=None, prefix = "", sufix=""):
    if date == None:
        date = pd.Timestamp.now(tz='America/Los_Angeles')
    file_name = file_date()
    return prefix + file_name + sufix + '.json'

def get_file(file_path, default = []):
    add_to_file = Path(file_path)
    if add_to_file.is_file():
        with open(add_to_file) as json_data:
                statuses = json.load(json_data)
    else:
        statuses = default
    return statuses

def write_file(path, data):
    log("writing to: " + str(path))
    with open(path, 'w') as outfile:
        json.dump(data, outfile)



def write_files_dict(day_dict, dir):
    for key,val in day_dict.items():
        day_name = str(key) + ".json"
        write_to_s3(val, day_name, directory=dir)


def update_dict(dt, key, value):
    if key in dt:
        dt[key].append(value)
    else:
        dt[key] = [value]
    return dt
        
def make_local(d):
    dd = pd.to_datetime(d)
    return dd.tz_localize('UTC').tz_convert('America/Los_Angeles')

def log(string, title=""):
    print(title + str(string))

### Data Utils
def get_quoted_status(status):
    quoted_status = {}
    if "retweeted_status" in status:
        quoted_status = status['retweeted_status']
    elif "is_quote_status" in status and status["is_quote_status"] == True:
        if "quoted_status" in status:
            quoted_status = status["quoted_status"]
        else:
            print("? atypical status embed")
    return quoted_status

# def get_quoted_status(status):
#     quoted_status = {}
#     if "is_quote_status" in status and status["is_quote_status"] == True:
#         if "quoted_status" in status:
#             quoted_status = status["quoted_status"]
#         elif "retweeted_status" in status and 'quoted_status' in status["retweeted_status"]:
#             quoted_status = status['retweeted_status']['quoted_status']
#         else:
#             print("?")
#     return quoted_status

def twitter_url_to_id(url):
    ## turn twitter URLs into Twitter IDs so the IDs can be aggregated
    return_label = url
    if url.find("https://twitter.com/") > -1:
        m = re.findall('https://twitter.com/.*/status/(\d*)|https://twitter.com/i/moments/(\d*)',url)
        if len(m) > 0:
            n = m[0][0] if m[0][0] != '' else m[0][1]
            return_label = str(n)
    return return_label

def clean_query_params(url):
    if not isinstance(url, string_types):
        return url
    return_label = url
    sites_need_params = ["news.ycombinator.com", "youtube", "quantocracy", "abcnews.go.com", "c-span", "twitter.com/search"]
    if not any(ext in return_label for ext in sites_need_params):
        return_label = re.sub('(\?|\#).*','',return_label)
    return return_label

def un_amp(link):
    if not isinstance(link, string_types):
        return link
    if link.find("/amp") == -1 and link.find(".amp")==-1:
        return link
    new_link = link.replace("//amp.", "//www.")
    match = re.search('(https?://)www.*?/(www.*)', new_link)
    if match:
        new_link = match.group(1)+match.group(2)
    new_link = new_link.replace("//www.google.com/amp/s/", "//")
    new_link = new_link.replace("/amphtml/", "/")
    new_link = re.sub("/amp/?", "/", new_link)
    print("unamped link: " + new_link)
    return new_link


def clean_one(status):
    url_indices = []
    media_indices = []
    if 'urls' in status['entities']:
        url_indices = [mention['indices'] for mention in status["entities"]["urls"]]
    if 'media' in status['entities']:
        media_indices = [media['indices'] for media in status["entities"]["media"]]
    indices = url_indices + media_indices
    
    clean_text = status["full_text"]
    sorted_indices = sorted(indices, key=lambda tup: tup[0], reverse=True)
    for index in sorted_indices:
        clean_text = clean_text[:index[0]] + clean_text[index[1]:]
    return clean_text

def clean_title(s):
    if s and isinstance(s, str):
        s = re.sub("\n"," ",s)
        s = re.sub("\s{2,}"," ",s)
    return s

def getDefaultSettings():
    t = Twitter(
        auth=OAuth('22330215-FbOCxaVolV3MnEjNlUHiHdt72A3lTrhgRbW7jN3Vu',
                   'SuKCrFN39xdqOXMCLQuuQFeAX3lNBrxq0QHfQtWFbEjrd',
                   'OZO7eEaK3YNb19Weflt9jytdQ',
                   'oEoYGe0mliUB924JlFsz97bFwvhQpdqJao577FsYhVvy3r644w'))
    # s3dir = 'data-aws/gen_two/'
    s3dir = 'data-aws/test_dir_om_2/'
    settings = {"t":t, "s3dir":s3dir}
    return settings

def key_exists(key_chain, obj):
    cur_obj = obj
    for i, key in enumerate(key_chain):
        if key in cur_obj:
            if i == len(key_chain) -1:
                return True
            else:
                cur_obj = cur_obj[key]
        else:
            return False

def valid_list_index(key_candidate, list_candidate):
    return isinstance(list_candidate, list) and isinstance(key_candidate, int) and key_candidate < len(list_candidate)

# def val_or_default(key_chain, obj, default=None):
#     cur_obj = obj
#     for i, key in enumerate(key_chain):
#         if key in cur_obj or valid_list_index(key, cur_obj):
#             if i == len(key_chain) -1:
#                 return cur_obj[key]
#             else:
#                 cur_obj = cur_obj[key]
#         else:
#             return default

def list_ize(val):
    if not isinstance(val, list):
        return [val]
    else:
        return val


def val_or_default2(key_chain, obj, default=[]):
    #Key_chain like this: ["extended_entities", "media", [], "video_info", "variants", [0], "url"]
    cur_obj = obj
    for i, key in enumerate(key_chain):
        if isinstance(cur_obj, list) and key == []:
            if i == len(key_chain) - 1:
                return list_ize(cur_obj[key])
            else:
                retval = []
                for ix in range(len(cur_obj)):
                    retval.extend(
                        val_or_default2(key_chain[i + 1:], cur_obj[ix],
                                       default))
                return retval
        elif key in cur_obj or valid_list_index(key, cur_obj):
            if i == len(key_chain) - 1:
                return list_ize(cur_obj[key])
            else:
                cur_obj = cur_obj[key]
        else:
            return default

twj_specs = {
    "urls": ["entities", "urls", [], "expanded_url"],
    "hashes": ["entities", "hashtags", [], "text"],
    "video": ["extended_entities", "media", [], "video_info", "variants", -1, "url"],
    "image_ref": ["extended_entities", "media", [], "media_url"],
    "embed": ["quoted_status_id"]
}