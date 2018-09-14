import re
from process_urls import process_url_list
import utils

def uate_dict_nodup(dt, key, value):
    if key in dt:
        # Experimental: Each user only gets one tweet per label
        users = [status["user"]["id_str"] for status in dt[key]]
        if value["user"]["id_str"] not in users:
            dt[key].append(value)
    else:
        dt[key] = [value]
    return None

def add_batch(batch_enhanced_full, batch_enhanced):
    batch_enhanced_full.extend(batch_enhanced)
    print("Length of enhanced post batch: " + str(len(batch_enhanced_full)))
    
    return batch_enhanced_full

def make_filtered_label_dict(batch_enhanced_full, threshhold = 2):
    # batch_enhanced_full.extend(batch_enhanced)
    label_dict = {}
    for status in batch_enhanced_full:
        norm_labels = utils.val_or_default2(["extended_entities", "media", [], "media_url"],status)
        # norm_labels = status["satellite_enhanced"]["combined_labels"]
        if len(norm_labels) > 0:
            uate_dict_nodup(label_dict, norm_labels[0], status)
    filt_label_dict =  {str(l): {"statuses":v, "count":len(v)} for l, v in label_dict.items() if len(v) >= threshhold}
    for k, v in filt_label_dict.items():
        print(k, str(len(v)))

    return filt_label_dict
    
def control(batch_enhanced,s):
    # statuses_enhanced = utils.read_from_s3(utils.file_name( sufix = "_enhanced"), directory='data-aws/gen_two/')
    batch_enhanced_full = utils.read_from_s3(utils.file_name( sufix = "_batch_enhanced_full"), seed=[], directory=s["s3dir"])
    print("Length of enhanced pre batch: " + str(len(batch_enhanced_full)))
    # enhanced_w_batch = add_batch(batch_enhanced_full, batch_enhanced)
    # utils.write_to_s3(enhanced_w_batch, utils.file_name( sufix = "_batch_enhanced_full"), directory=s["s3dir"])
    # utils.write_to_s3(processed_list, utils.file_name( sufix = "_processed_list"), directory=s["s3dir"])

    filtered_label_dict = make_filtered_label_dict(batch_enhanced_full)

    # utils.write_to_s3(filtered_label_dict, utils.file_name( prefix = "_batch_filt_label_dict_enhanced_"), directory=s["s3dir"])
    return filtered_label_dict

if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    batch_enhanced = utils.read_from_s3(utils.file_name( sufix = "_batch_enhanced_d"), directory=sd["s3dir"])
    # statuses_enhanced = utils.read_from_s3(utils.file_name( sufix = "_batch_enhanced_full"), seed=[], directory=sd["s3dir"])
    control(batch_enhanced, sd)