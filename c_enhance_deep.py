import utils
import time

def get_embedded_id(stat):
    quoted_status = utils.get_quoted_status(stat)
    embed = utils.val_or_default2(["quoted_status_id"],quoted_status)
    first = utils.val_or_default2(["quoted_status_id"],stat)
    first_urls = utils.val_or_default2(["entities", "urls", [], "expanded_url"],stat)
    return first, embed, first_urls


def process_urls(url_list):
    twitter_links = []
    urls = []
    for url in url_list:
        if url.find("https://twitter.com/") > -1:
            twitter_links.append(utils.twitter_url_to_id(url))
        else:
            urls.append(utils.clean_query_params(url))
#	 return twitter_links, urls
    return urls


def get_next_level(status, s):
    first, embed, first_urls = get_embedded_id(status)
    try:
        # Should only be one embed
        embed_status = s["t"].statuses.show(id=embed[0], tweet_mode="extended")
        this_embed, next_embed, this_urls = get_embedded_id(embed_status)
    except Exception as exc:
        this_embed, next_embed, this_urls = [], [], []
        embed_status = {}
        utils.log(exc, "Error in getting deep status: ")
    
    if len(this_urls) > 0:
        this_urls = process_urls(this_urls) 

    time.sleep(.1)
    return this_embed, next_embed, this_urls, embed_status


def trace_links_down(statuses_enhanced, s):
    existing_cnt = 0
    for quoted_status in statuses_enhanced:
        lb = quoted_status["satellite_enhanced"]["labels"]

        if "quoted_labels_links_deep" not in lb and "quoted_labels_twrefs_deep" not in lb:
            root = quoted_status
            count = 0
            max_embed = []
            max_url = []
            next_embed = quoted_status["satellite_enhanced"]["labels"][
                "quoted_labels_twrefs"]
            while next_embed != []:
                initial_id = next_embed
                count += 1
                this_embed, next_embed, this_urls, quoted_status = get_next_level(quoted_status, s)
                qt = utils.get_quoted_status(quoted_status)
                # print(initial_id, this_embed, next_embed, this_urls, count)
                if this_urls != []:
                    max_url = this_urls
                max_embed = this_embed
            #for links, add to "labels" so we can do the short url processing after
            #for refs, create entry "labels_proc so we preserve the original ref and can store the quoted status.
            root["satellite_enhanced"]["labels"]["quoted_labels_links_deep"] = max_url
            root["satellite_enhanced"]["labels"]["quoted_labels_twrefs_deep"] = max_embed
            if max_embed != None and max_embed != []:
                root["satellite_enhanced"]["labels"]["quoted_labels_twrefs_deep_status"] = qt
        else:
            existing_cnt += 1
    utils.log(existing_cnt, "Existing Count: ")
    return statuses_enhanced


def control(batch_enhanced, s):
    utils.log("", "Starting deep trace")
    # batch_enhanced = utils.read_from_s3(utils.file_name( sufix = "_enhanced"), directory="data-aws/gen_two/")
    batch_enhanced = trace_links_down(batch_enhanced, s)
    # utils.write_to_s3(
    #     batch_enhanced,
    #     utils.file_name(batch_enhanced, sufix="_batch_enhanced_c"),
    #     directory=s["s3dir"])
    return batch_enhanced


if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    batch_enhanced = utils.read_from_s3(
        utils.file_name(sufix="_batch_enhanced"), directory=sd["s3dir"])

    control(batch_enhanced, sd)
 