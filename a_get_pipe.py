import time
import datetime 
import utils

def get_min_max(id_list):
    if len(id_list) == 0:
        return 0,0
    max_id = max(id_list)
    min_id = min(id_list)
    print(len(id_list), max_id, min_id)
    return min_id, max_id

def process_results(statuses):
    if len(statuses) > 0:
        min_id = int(statuses[-1]["id"])-1
        maxi_id = int(statuses[0]["id"])
        utils.log(str(min_id) + ", " + str(maxi_id), "Min max IDs: ")
        return maxi_id, min_id
    else:
        utils.log("no statuses")
        return 0, 0

def get_new(exist_max_id,s):
    new_statuses = []
    chunks = 12
    if "list_id" in s and s["list_id"] != None:
        print("List Id: " + str(s["list_id"]))
        statuses = s["t"].lists.statuses(count = 200, trim_user=True, tweet_mode="extended", list_id=s["list_id"], include_rts=True)
    else:
        print("Home Timeline")
        statuses = s["t"].statuses.home_timeline(count = 200, trim_user=True, tweet_mode="extended")
    maxi_id, min_id = process_results(statuses)
    if len(statuses) > 0:
        new_statuses.extend(statuses)

    for chunk in range(chunks-1):
        if min_id < exist_max_id or maxi_id == 0:
            break
        time.sleep(1)
        if "list_id" in s and s["list_id"] != None:
            print("List Id: " + str(s["list_id"]))
            statuses = s["t"].lists.statuses(count = 200, trim_user=True, max_id=min_id, tweet_mode="extended", list_id=s["list_id"], include_rts=True)
        else:
            print("Home Timeline")
            statuses = s["t"].statuses.home_timeline(count = 200, trim_user=True, max_id=min_id, tweet_mode="extended")
        maxi_id, min_id = process_results(statuses)
        if len(statuses) > 0:
            new_statuses.extend(statuses)
    # statuses_existing.extend(new_statuses)
    return new_statuses

def dedup(new_statuses, existing_status_list):
    #Could probably be index based, but more fault-tolorant this way
    seen = set(existing_status_list)
    reduced = []
    for stat in new_statuses:
        k = int(stat["id"])
        if k not in seen:
            reduced.append(stat)
    utils.log(len(new_statuses), "Initial Batch: ")
    utils.log(len(reduced), "Reduced Batch: ")
    return reduced

def filter_on_day(statuses):
    statuses_filtered = []
    current_day_key = utils.file_date()
    for stat in statuses:
        dd = utils.make_local(stat["created_at"])
        key = str(dd.day) + "-" + str(dd.month) + "-" + str(dd.year)
        if key == current_day_key:
            stat["created_at_tz"] = str(dd)
            statuses_filtered.append(stat)
    filtered_cnt = len(statuses) - len(statuses_filtered)
    if filtered_cnt > 0:
        print(str(filtered_cnt) + " statuses filtered for not being in day " + current_day_key)
    return statuses_filtered

def make_processed_list(s):
    processed_list = utils.read_from_s3("processed_list", seed=[],directory=s["s3dir"])
    # if not processed_list:
    #     statuses_enhanced = utils.read_from_s3(utils.file_name( sufix = "_enhanced"), directory=s["s3dir"])
    #     processed_list = [int(status["id"]) for status in statuses_enhanced]
    return processed_list

def update_processed_list(processed_list, deduped_status_batch):
    batch_processed_list = [int(status["id"]) for status in deduped_status_batch]
    max_index = 800 if len(processed_list) > 800 else len(processed_list)
    new_processed_list = processed_list[0:max_index]
    new_processed_list.extend(batch_processed_list)
    return new_processed_list

def control(s):
    processed_list = make_processed_list(s)
    exist_min_id, exist_max_id = get_min_max(processed_list)
    new_status_batch = get_new(exist_max_id, s)
    deduped_status_batch = dedup(new_status_batch, processed_list)
    updated_processed_list = update_processed_list(processed_list, deduped_status_batch)
    utils.write_to_s3(updated_processed_list, "processed_list", directory=s["s3dir"])
    date_filtered_batch = filter_on_day(deduped_status_batch)
    # utils.write_to_s3(date_filtered_batch, utils.file_name(sufix="_batch"), directory=s["s3dir"])
    return date_filtered_batch

if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    control(sd)



  
