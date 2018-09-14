import re
from process_urls import process_url_list
import utils

def uate_dict_nodup(dt, key, value):
    if key in dt:
        # Experimental: Each user only gets one tweet per label
        users = [s["user"]["id_str"] for s in dt[key]["statuses"]]
        if value["user"]["id_str"] not in users:
            dt[key]["statuses"].append(value)
    else:
        dt[key] = {"statuses":[value]}
    return None

def make_filtered_label_dict(batch_enhanced, label_dict, threshhold = 1):
    processed_list = set()
    for status in batch_enhanced:
        norm_labels = status["satellite_enhanced"]["combined_labels"]
        if len(norm_labels) > 0:
            # for label in norm_labels[0]: # Experimental: Choose first one
            uate_dict_nodup(label_dict, norm_labels[0], status)
    for l, v in label_dict.items():
        label_dict[l]["count"] = len(v)
        for s in label_dict[l]["statuses"]:
            processed_list.add(int(s["id"]))
    
    return label_dict, list(processed_list)
    
def control(batch_enhanced,s):    
    filtered_label_dict = utils.read_from_s3(utils.file_name( prefix = "_batch_filt_label_dict_enhanced_fld"), directory=s["s3dir"])
    filtered_label_dict, processed_list = make_filtered_label_dict(batch_enhanced, filtered_label_dict)
    utils.write_to_s3(filtered_label_dict, utils.file_name( prefix = "_batch_filt_label_dict_enhanced_fld"), directory=s["s3dir"])
    utils.write_to_s3(processed_list, utils.file_name( sufix = "_processed_list_fld"), directory=s["s3dir"])
    return filtered_label_dict

if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    batch_enhanced = utils.read_from_s3(utils.file_name( sufix = "_batch_enhanced_d"), directory=sd["s3dir"])
    # statuses_enhanced = utils.read_from_s3(utils.file_name( sufix = "_batch_enhanced_full"), seed=[], directory=sd["s3dir"])
    control(batch_enhanced, sd)