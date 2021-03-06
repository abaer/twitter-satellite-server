import re
from process_urls import process_url_list
import utils

def get_short_url_list(statuses):
    
    lookup_urls = set()
    for s in statuses:
        lb = s["satellite_enhanced"]["labels"]
        # candidates = lb["status_labels_links"] + lb["quoted_labels_links"] + lb["quoted_labels_links_deep"]
        candidates = select_deepest([lb["quoted_labels_links_deep"], lb["quoted_labels_links"], lb["status_labels_links"]])
        for url in candidates:
            m = re.findall('https?:\/\/.*?\.(.*?)/',url)
            if m and m[0] and len(m[0]) == 2:
                lookup_urls.add(url)
            elif url.find("bloom.bg") != -1:
                lookup_urls.add(url)
    print(str(len(lookup_urls)) + " in URL list")
    return list(lookup_urls)

def process_url(data, url, result_obj):
    ## CACHE
    ## Bloomberg special processing. Need way to know it's bad. 
    if len(data.history) > 0 and data.url.find("tosv2.html") > -1:
        expanded_url_full = data.history[-1].url
    else:
        expanded_url_full = data.url

    expanded_url = utils.clean_query_params(expanded_url_full)
    utils.add_to_cache(url, expanded_url, "short_urls", json_type=False, ex=432800)
    ## CACHE
    result_obj[url] = expanded_url
    return True

def process_short_urls(lookup_urls, s):
    shorten_dict = {}
    missing_urls = set()
    ## Find Cached URLs
    for url in lookup_urls:
        resolved = utils.get_from_cache(url, "short_urls", json_type=False)
        if resolved == None:
            missing_urls.add(url)
        else:
            shorten_dict[url] = resolved
    process_url_list(list(missing_urls), process_url, shorten_dict)
    return shorten_dict

def prioritize_links(twrefs, links):
    prioritized_vals = links if len(links) > 0 else twrefs
    return prioritized_vals

def select_deepest(lists):
    for list in lists:
        if len(list) > 0:
            return list
    return []

def make_combined_labels(stat):
    enh = stat["satellite_enhanced"]
    status_labels = prioritize_links(enh["labels"]["status_labels_twrefs"], enh["labels_proc"]["status_labels_links"])
    quoted_labels = prioritize_links(enh["labels"]["quoted_labels_twrefs"], enh["labels_proc"]["quoted_labels_links"])
    quoted_labels_deep = prioritize_links(enh["labels"]["quoted_labels_twrefs_deep"], enh["labels_proc"]["quoted_labels_links_deep"])

    combined_labels = select_deepest([quoted_labels_deep, quoted_labels, status_labels])

    un_amped_labels = [utils.un_amp(label) for label in combined_labels]
    try:
        combined_labels = list(set(un_amped_labels))
    except:
        print("problem with combined labels (d_process) ")
        print(enh)
        combined_labels = []
    return combined_labels

def add_to_statuses(shorten_dict, statuses):
    def process_list(url_list):
        return_list = []
        for url in url_list:
            if url in shorten_dict:
                return_list.append(shorten_dict[url])
            else:
                return_list.append(url)
        return return_list
    
    for stat in statuses:
        lb = stat["satellite_enhanced"]["labels"]
        status_labels_links = process_list(lb["status_labels_links"])
        quoted_labels_links = process_list(lb["quoted_labels_links"])  
        deep_links = process_list(lb["quoted_labels_links_deep"]) 
        return_labels = {"status_labels_links":status_labels_links, "quoted_labels_links":quoted_labels_links, "quoted_labels_links_deep":deep_links}
        
        stat["satellite_enhanced"]["labels_proc"] = return_labels
        stat["satellite_enhanced"]["combined_labels"] = make_combined_labels(stat)
    return statuses
    
def control(batch_enhanced, s):
    # batch_enhanced = utils.read_from_s3(utils.file_name( sufix = "_enhanced"), directory='data-aws/gen_two/')
    lookup_urls = get_short_url_list(batch_enhanced)
    shorten_dict = process_short_urls(lookup_urls, s)
    batch_enhanced = add_to_statuses(shorten_dict, batch_enhanced)
    # filtered_label_dict = make_filtered_label_dict(batch_enhanced)
    # utils.write_to_s3(batch_enhanced, utils.file_name( sufix= "_batch_enhanced_d"), directory=s["s3dir"])
    return batch_enhanced

if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    batch_enhanced = utils.read_from_s3(utils.file_name( sufix = "_batch_enhanced_c"), directory=sd["s3dir"])
    control(batch_enhanced, sd)