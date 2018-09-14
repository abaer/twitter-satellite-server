import json
import pandas as pd
import utils
import re

def pick_image(label_info):
    image_hierarchy = ["media_url", "image_ref", "profile_image_url"]
    final_image = ""
    for image_type in image_hierarchy:
        if label_info.get(image_type, "") != "":
            final_image = label_info[image_type]
            if image_type == "profile_image_url":
                try:
                    src_match = re.search('(.*)_normal(\..*)', final_image)
                    final_image = src_match.group(1) + src_match.group(2)
                except Exception as exc:
                    print("Problem with getting image", final_image)
            break
    return final_image 

def getKeyDate(item):
    return pd.to_datetime(item["date"])

def consume_twitter_label(li):
    tweat_id = li["id"]
    user_id = li["user"]
    # title = li["text"] + " [@" + li["screen_name"] + "]"
    title = li["text"]
    label = 'https://twitter.com/' + user_id + '/status/' + tweat_id
    return label, title

def process_label_dict(filt_label_dict, label_to_titles):
    threshhold = 2
    key = 1
    label_data = {}
    for label, group in filt_label_dict.items():
        if group["count"] >= threshhold:
            if utils.key_exists(["label_info", "type"], group) and group["label_info"]["type"] == "twitter_id":
            # if "label_info" in group and "type" in group["label_info"] and group["label_info"]["type"] == "twitter_id":
                label_info = group["label_info"]
                link, title = consume_twitter_label(label_info)
            else:
                thumb_test = utils.key_exists([label, 'news_image', 'media_url_news_thumb'], label_to_titles)
                media_url = label_to_titles[label]["news_image"]["media_url_news_thumb"] if thumb_test else ""
                image_ref = ""
                if media_url == "":
                    for s in group["statuses"]:
                        image_ref = utils.val_or_default2(["extended_entities", "media", [0], "media_url"], s, default="")
                        if image_ref != "":
                            break
                # media_url2 = val_or_default2([label, 'news_image', 'media_url_news_thumb'], label_to_titles, default="")
                in_data = True if label in label_to_titles else False
                title = label_to_titles[label]["parsed_title"] if in_data and "parsed_title" in label_to_titles[label] else label
                domain = label_to_titles[label]["domain"] if in_data and "domain" in label_to_titles[label] else ""
                domainless_title = label_to_titles[label]["domainless_title"] if in_data and "domainless_title" in label_to_titles[label] else label
                label_info = {
                    "type" : "external_link",
                    "media_url" : media_url,
                    "image_ref" : image_ref,
                    "domain" : domain,
                    "domainless_title" : domainless_title
                }
                link = label 
            final_image_url = pick_image(label_info)
            row = {"label_info":label_info, "title":title, "count":group["count"], 'key':key, 'tag':link, 'final_image_url':final_image_url}
            label_data[label] = row
            key += 1
    return label_data

def label_data_to_key(label_data):
    key_data = {v["key"]: v for k, v in label_data.items()}
    return key_data

def make_json_statuses(filt_label_dict, label_to_key):
    threshhold = 2
    statuses = []
    for label, group in filt_label_dict.items():
        if group["count"] >= threshhold:
            for s in group["statuses"]:
                label_count = group["count"]
                count = s["retweet_count"] + s["favorite_count"]
                #                 date = pd.to_datetime(s["created_at"], errors='coerce').isoformat()
                date = pd.to_datetime(s["created_at"], errors='coerce').tz_localize('UTC').tz_convert('America/Los_Angeles')
                date = int(round(date.timestamp() * 1000))
                screen_name = s["satellite_enhanced"]["name_top"]["screen_name"] if "name_top" in s["satellite_enhanced"] else s["user"]["id_str"]
                profile_image_url = s["satellite_enhanced"]["name_top"]["profile_image_url"] if utils.key_exists(["satellite_enhanced", "name_top", "profile_image_url"],s) else None
                key = label_to_key[label]["key"]
                clean_full_text = utils.clean_one(s)
                clean_full_text = utils.clean_title(clean_full_text)
                row = {
                    'key': key,
                    "count": count,
                    'date': date,
                    'label_count': label_count,
                    # 'text': s['full_text'],
                    'text': clean_full_text,
                    'user_id': s["user"]["id_str"],
                    'tweet_id': s['id_str'],
                    'screen_name': screen_name,
                    'profile_image_url': profile_image_url
                }
                statuses.append(row)
    statuses_sorted = sorted(statuses, key=getKeyDate)
    return statuses_sorted

def control(filt_label_dict, label_to_titles, s):
    utils.log(str(len(filt_label_dict)) + " loaded, " + str(len(label_to_titles)) + " titles loaded")
    label_data = process_label_dict(filt_label_dict, label_to_titles)
    statuses = make_json_statuses(filt_label_dict, label_data)
    keyed_label_data = label_data_to_key(label_data)
    final = {"statuses": statuses, "labels": keyed_label_data}
    name = "d3-" + utils.file_date() + ".json"
    utils.write_to_s3(
        json.dumps(final),
        name,
        directory=s["s3dir"] + 'production/',
        public=True)
    # label_set = list(label_data.keys())
    # label_set_name = "label_set_" + utils.file_date() + ".json"
    # utils.write_to_s3(
    #     label_set,
    #     label_set_name,
    #     directory=s["s3dir"] + 'production/',
    #     public=True,)
    return None

if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    filt_label_dict = utils.read_from_s3(utils.file_name(prefix='_batch_filt_label_dict_enhanced_'), directory=sd["s3dir"])
    label_to_titles = utils.read_from_s3('batch_titles_fd_' + utils.file_date() + '.json', directory=sd["s3dir"])
    control(filt_label_dict, label_to_titles, sd)
