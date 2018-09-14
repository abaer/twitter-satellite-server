import re
import utils
import pandas as pd
import json

def get_labels(status):
    if not bool(status):
        return [], []
#     hashes = utils.find(["entities", "hashtags","text"], status)
#     hashes = ["#"+h for h in hashes]
    urls = utils.val_or_default2(["entities", "urls", [], "expanded_url"], status, default=[])
    # VIDEO CODE
    # if "extended_entities" in status and "media" in status["extended_entities"] and  "video_info" in status["extended_entities"]["media"][0]:
    #     video_info = status["extended_entities"]["media"][0]["video_info"]
    #     if video_info != [] and video_info != None:
    #         variants = video_info["variants"]
    #         video_url = variants[len(variants) - 1]["url"]
    #         if video_url and (not urls or urls == []):
    #             urls = [video_url]
    #             print("Video Urls Found!" + video_url)
    # VIDEO CODE
    embed_fin = utils.val_or_default2(["quoted_status_id"], status, default=[])
    return urls, embed_fin


def process_twitter_label(l):
    return_label = str(l)

    ## turn twitter URLs into Twitter IDs so the IDs can be aggregate
    if return_label.find("https://twitter.com/") > -1:
        return_label = utils.twitter_url_to_id(return_label)

    # Remove query args from URLs so ad params don't keep them from aggregating
    if re.findall('^https://.*\?|^http://.*\?', return_label):
        return_label = utils.clean_query_params(return_label)

    return return_label


def add_time_zone_date(s):
    d = s["created_at"]
    dd = pd.to_datetime(d)
    dd_tz = dd.tz_localize('UTC').tz_convert('America/Los_Angeles')
    return str(dd_tz)


def get_combined_labels(s):
    # Get status of 2nd level (quoted or retweeted) tweet
    quoted_status = utils.get_quoted_status(s)

    # Get labels for 1st level and 2nd level tweets
    status_labels_urls, status_labels_embeds = get_labels(s)
    quoted_labels_urls, quoted_labels_embeds = get_labels(quoted_status)

    #watch for ref numbers in in links without corresponding refs
    status_labels_links = set(
        [process_twitter_label(l) for l in status_labels_urls])
    status_labels_twrefs = set([str(l) for l in status_labels_embeds])
    quoted_labels_links = set(
        [process_twitter_label(l) for l in quoted_labels_urls])
    quoted_labels_twrefs = set([str(l) for l in quoted_labels_embeds])

    #remove links if the duplicated refs
    status_labels_links = status_labels_links.difference(status_labels_twrefs)
    quoted_labels_links = quoted_labels_links.difference(quoted_labels_twrefs)

    return_lables = {
        "status_labels_links": list(status_labels_links),
        "status_labels_twrefs": list(status_labels_twrefs),
        "quoted_labels_links": list(quoted_labels_links),
        "quoted_labels_twrefs": list(quoted_labels_twrefs)
    }
    return return_lables

def filter_on_day(stat):
    current_day_key = utils.file_date()
    dd = utils.make_local(stat["created_at"])
    key = str(dd.day) + "-" + str(dd.month) + "-" + str(dd.year)
    return_bool = key == current_day_key
    return return_bool

def enhance(batch_enhanced):
    filtered_enhanced_batch = []
    utils.log(len(batch_enhanced), "Number batch statuses: ")
    for stat in batch_enhanced:
        enhanced = {}
        enhanced["created_at_tz"] = add_time_zone_date(stat)
        enhanced["labels"] = get_combined_labels(stat)
        ref_cnt = 0
        for key, val in enhanced["labels"].items():
            ref_cnt += len(val) 
        if ref_cnt > 0:
            stat["satellite_enhanced"] = enhanced
            filtered_enhanced_batch.append(stat)
    utils.log(len(filtered_enhanced_batch), "Number enhanced batch statuses: ")
    return filtered_enhanced_batch


def control(date_filtered_batch, s):
    # statuses_enhanced = utils.read_from_s3(utils.file_name(sufix="_enhanced"), directory=s["s3dir"], seed=[])
    batch_enhanced = enhance(date_filtered_batch)
    # utils.write_to_s3(
    #     batch_enhanced,
    #     utils.file_name(sufix="_batch_enhanced_full_testxxx"),
    #     directory=s["s3dir"])
    return batch_enhanced


if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    date_filtered_batch = utils.read_from_s3(utils.file_name(sufix="_batch_enhanced_full"), directory=sd["s3dir"])
    control(date_filtered_batch, sd)