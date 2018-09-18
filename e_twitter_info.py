import utils


def get_twitter_info(label, stat):
    quoted_status = utils.get_quoted_status(stat)
    if "quoted_labels_twrefs_deep_status" in stat["satellite_enhanced"]["labels"]:
        quoted_status = stat["satellite_enhanced"]["labels"]["quoted_labels_twrefs_deep_status"]
        print("Found deep status")
    if "full_text" in quoted_status:
        clean_fill_text = utils.clean_one(quoted_status)
        return_label = utils.clean_title(clean_fill_text)
        ## NEW IMAGE REF
        image_ref = utils.val_or_default2(["extended_entities", "media", 0, "media_url"], quoted_status, default="")
        ## NEW IMAGE REF
        post_data = {
            "type": "twitter_id",
            "id": str(quoted_status["id"]),
            "user": quoted_status["user"]["id_str"],
            "text": return_label,
            "image_ref": image_ref
        }
    else:
        post_data = {}
    return post_data


def make_user_chunks(users, n=100):
    chunks = [users[i:i + n] for i in range(0, len(users), n)]
    return chunks


def do_user_lookup(users, s):
    ts = ",".join(users)
    users_lookup = s["t"].users.lookup(user_id=str(ts))
    user_dict_lookup = {}
    for user in users_lookup:
        user_id = user["id_str"]
        row = {
            "name": user["name"],
            "screen_name": user["screen_name"],
            "profile_image_url": user["profile_image_url"]
        }
        user_dict_lookup[user_id] = row
        ## CACHE
        utils.add_to_cache(user_id, row, "user_data", ex=604800)
        ## CACHE
    utils.log(user_dict_lookup, "User Results: ")
    return user_dict_lookup


def update_dict_with_user_info(filtered_label_dict, user_data):
    for l, val in filtered_label_dict.items():
        #Update statuses
        for stat in val["statuses"]:
            user_id = stat["user"]["id_str"]
            if user_id in user_data:
                stat["satellite_enhanced"]["name_top"] = user_data[user_id]
            else:
                stat["satellite_enhanced"]["name_top"] = user_id
        #update labels
        if str(l).isdigit() and 'user' in val["label_info"]:
            user = val["label_info"]["user"]
            if user in user_data:
                val["label_info"]["screen_name"] = user_data[user]["screen_name"]
                if "profile_image_url" in user_data[user]:
                    val["label_info"]["profile_image_url"] = user_data[user]["profile_image_url"]
                else:
                    utils.log("no profile_image_url for " + user)
                    val["label_info"]["profile_image_url"] = None

    return filtered_label_dict


# def update_user_info(filtered_label_dict, user_data, s):
def update_user_info(filtered_label_dict, s):
    all_users = set()
    for l, val in filtered_label_dict.items():
        #Do statuses
        label_users = [stat["user"]["id_str"] for stat in val["statuses"]]
        all_users.update(label_users)
        #Do Labels
        # if str(l).isdigit():
        if str(l).isdigit() and "label_info" not in val:
            post_data = get_twitter_info(l, val["statuses"][-1])
            filtered_label_dict[l]["label_info"] = post_data
            if "user" in post_data:
                all_users.add(post_data["user"])
    # all_users_lookup = [u for u in all_users if u not in user_data]
    ## Cache code##
    user_data, all_users_lookup = utils.get_from_cache_m(all_users, "user_data")
    utils.log(len(all_users), "Number total users in set: ")
    utils.log(len(all_users_lookup), "Number users needing lookup: ")
    if len(all_users_lookup) > 0:
        user_chunks = make_user_chunks(all_users_lookup, 100)
        for this_lookup in user_chunks:
            user_dict_lookup = do_user_lookup(this_lookup, s)
            user_data.update(user_dict_lookup)
    return user_data, filtered_label_dict


def control(filtered_label_dict, s):
    user_data, filtered_label_dict = update_user_info(filtered_label_dict, s)
    filtered_label_dict = update_dict_with_user_info(filtered_label_dict, user_data)
    utils.write_to_s3(
        filtered_label_dict,
        utils.file_name(prefix='_batch_filt_label_dict_enhanced_'),
        directory=s["s3dir"])
    return filtered_label_dict


if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    filtered_label_dict = utils.read_from_s3(
        utils.file_name(prefix='_batch_filt_label_dict_enhanced_'),
        directory=sd["s3dir"])
    control(filtered_label_dict, sd)