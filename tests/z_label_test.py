from pathlib import Path
import json
import utils
from functools import reduce
import re
import pandas as pd

#translient
s = utils.getDefaultSettings()
cache_on = False

specs = {
    "urls": ["entities", "urls", [], "expanded_url"],
    "hashes": ["entities", "hashtags", [], "text"],
    "video": ["extended_entities", "media", [], "video_info", "variants", -1, "url"],
    "image_ref": ["extended_entities", "media", [], "media_url"],
    "embed": ["quoted_status_id"]
}


def get_labels(status, seed=None):
    if seed == None:
        seed = {t: [] for t, v in specs.items()}
    if not bool(status):
        return seed
    for ref_type, spec in specs.items():
        status_id = status["id_str"]
        ref_list = utils.val_or_default2(spec, status, default=[])
        rows = [{"source":status_id, "label":l} for l in ref_list]
        seed[ref_type].extend(rows)
        # seed[ref_type].extend(utils.val_or_default2(spec, status, default=[]))
    return seed


def process_links(url_list):
    return_urls = []
    return_trefs = []
    for l in url_list:
        candidate = str(l["label"])
        ## turn twitter URLs into Twitter IDs so the IDs can be aggregate
        if candidate.find("https://twitter.com/") > -1:
            return_trefs.append(int(utils.twitter_url_to_id(candidate)))
        # Remove query args from URLs so ad params don't keep them from aggregating
        elif re.findall('^https:\/\/.*|^http:\/\/.*', candidate):
            return_urls.append(utils.clean_query_params(candidate))
    return return_urls, return_trefs


def add_time_zone_date(s):
    d = s["created_at"]
    dd = pd.to_datetime(d)
    dd_tz = dd.tz_localize('UTC').tz_convert('America/Los_Angeles')
    return str(dd_tz)


def get_labels_at_level(stat, passedRefs):
    refs = get_labels(stat, passedRefs)
    quoted_status = utils.get_quoted_status(stat)
    refs_quoted = get_labels(quoted_status, seed=refs)
    return refs_quoted

def get_combined_labels(stat, level = 1):
    refs_quoted = get_labels_at_level(stat, None)
    while len(refs_quoted["embed"]) == 2*level:
        embed_status = None
        level +=1
        try:
            embed_id = str(refs_quoted["embed"][-1]["label"])
            if cache_on:
                embed_status = utils.get_from_cache(embed_id, "tweets_by_id", print_hits=True)
            if embed_status == None:
                embed_status = s["t"].statuses.show(id=embed_id, tweet_mode="extended")
                if cache_on:
                    utils.add_to_cache(embed_id, embed_status, "tweets_by_id")
            print("Before: ")
            print(len(refs_quoted["urls"]), len(refs_quoted["video"]), len(refs_quoted["image_ref"]), len(refs_quoted["embed"]))
            refs_quoted = get_labels_at_level(embed_status, refs_quoted)
            print("After: ")
            print(len(refs_quoted["urls"]), len(refs_quoted["video"]), len(refs_quoted["image_ref"]), len(refs_quoted["embed"]))
        except Exception as exc:
            print("Couldn't get deep status " + str(refs_quoted["embed"][-1]["label"]))
            print(exc)
            continue


    # while len(trefs) == 2*level, get next 2 levels
    # refs = get_labels(stat, passedRefs)
    # quoted_status = utils.get_quoted_status(stat)
    # refs_quoted = get_labels(quoted_status, seed=refs)
    processed_urls, trefs = process_links(refs_quoted["urls"])
    
    refs_quoted["processed_urls"] = processed_urls
    # refs_quoted["processed_trefs"] = list(set(refs_quoted["embed"] + trefs))
    # if level > 1:
        # print(len(refs_quoted["embed"]), str(level))
        # print(refs_quoted)
    return refs_quoted


def enhance(batch):
    filtered_enhanced_batch = []
    utils.log(len(batch), "Number batch statuses: ")
    for stat in batch:
        enhanced = {
            "created_at_tz": add_time_zone_date(stat),
            "labels": get_combined_labels(stat)
        }
        for val in enhanced["labels"].values():
            if len(val) > 0:
                stat["satellite_enhanced"] = enhanced
                filtered_enhanced_batch.append(stat)
                #IF THERE ARE ANY REFERENCES, WE"LL SAVE - NO NEED TO CHECK FURTHER
                # print(enhanced)
                break 


    utils.log(len(filtered_enhanced_batch), "Number enhanced batch statuses: ")
    return filtered_enhanced_batch



fn = '/Users/alanbaer/Downloads/27-7-2018_enhanced.json'
statuses = utils.get_file(fn)
filtered_enhanced_batch = enhance(statuses)
# utils.write_file('/Users/alanbaer/Downloads/27-7-2018_enhanced_test.json', filtered_enhanced_batch)

# embed_cnt = 0
# tref_cnt = 0
# for s in filtered_enhanced_batch:
#     if len(s["satellite_enhanced"]["labels"]["embed"]) >1:
#         embed_cnt += 1
#     if len(s["satellite_enhanced"]["labels"]["processed_trefs"]) >1:
#         tref_cnt +=1
#     # if len(s["satellite_enhanced"]["labels"]["video"]) >0:
#     #     print(s["satellite_enhanced"]["labels"]["video"])
# print(embed_cnt, tref_cnt)

