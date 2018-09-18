from twitter import Twitter, OAuth

import a_get_pipe
import b_enhance
import c_enhance_deep
import d_process_shorts
import d2_full_enhanced
import e_twitter_info
import f_titles
import f2_images
import g_produce
import g_produce_prime
import time
from threading import Thread  

alant = Twitter(
auth=OAuth('22330215-FbOCxaVolV3MnEjNlUHiHdt72A3lTrhgRbW7jN3Vu',
            'SuKCrFN39xdqOXMCLQuuQFeAX3lNBrxq0QHfQtWFbEjrd',
            'OZO7eEaK3YNb19Weflt9jytdQ',
            'oEoYGe0mliUB924JlFsz97bFwvhQpdqJao577FsYhVvy3r644w'))
tx = Twitter(
    auth=OAuth('1016457979703390208-NoKu9rFviudg0o0aQbOWLcllV9qI9Y',
                'qomxXHpgG4ZXE5749DDe06cojLJGNspeXbXGh1XwmRQCx',
                'wCs284AjAryGTOrTqt2g7OCOz',
                'dDnxDeRdkfZJOau84GzMJNgtDUwe8KR2CRBTj7BUjY6DHUH0RT'))
settings_alan = {
    "s3dir": 'data-aws/gen_two/',
    "name": "Alan",
    "t" : alant
}

settings_tapbot = {
    "s3dir": 'data-aws/test_dir_tapbot/',
    "list_id": "1016459500579180544",
    "name": "tapbot",
    "t" : tx
}

settings_ingrahm = {
    "s3dir": 'data-aws/test_dir_ingraham/',
    "list_id": "1017587673698111488",
    "name": "Laura Ingraham",
    "t" : tx
}

settings_jd = {
    "s3dir": 'data-aws/test_dir_d/',
    "list_id": "1016484051295920133",
    "name": "Jeff Dean",
    "t" : tx
}

settings_kv = {
    "s3dir": 'data-aws/test_dir_kv/',
    "list_id": "1017171017452675073",
    "name": "Kayvon Beykpour",
    "t" : tx
}

settings_ottolenghi = {
    "s3dir": 'data-aws/test_dir_ottolenghi/',
    "list_id": "1018690219305029633",
    "name": "Ottolenghi",
    "t" : tx
}

settings_kaveh = {
    "s3dir": 'data-aws/test_dir_akbar_2/',
    "list_id": "1018863470798888960",
    "name": "Kaveh Akbar",
    "t" : tx
}


settings_om = {
    "list_id": "1019753530717024258",
    "s3dir": 'data-aws/test_dir_om_2/',
    "name": "OM",
    "t":tx,
}

settings_jardine = {
    "list_id": "1021477742699786240",
    "s3dir": 'data-aws/test_dir_jardine/',
    "name": "Jardine",
    "t":tx,
}

settings_jack = {
    "list_id": "1026938305827545088",
    "s3dir": 'data-aws/dir_jack/',
    "name": "Jack",
    "t":tx,
}

settings_abrams = {
    "list_id": "1041031320540000262",
    "s3dir": 'data-aws/dir_abrams/',
    "name": "Abrams",
    "t":tx,
}

def update_satellite(s):
    # tx = Twitter(
    #     auth=OAuth('1016457979703390208-NoKu9rFviudg0o0aQbOWLcllV9qI9Y',
    #                'qomxXHpgG4ZXE5749DDe06cojLJGNspeXbXGh1XwmRQCx',
    #                'wCs284AjAryGTOrTqt2g7OCOz',
    #                'dDnxDeRdkfZJOau84GzMJNgtDUwe8KR2CRBTj7BUjY6DHUH0RT'))
    # s["t"] = tx
    date_filtered_batch = a_get_pipe.control(s) 
    batch_enhanced = b_enhance.control(date_filtered_batch, s)
    batch_enhanced = c_enhance_deep.control(batch_enhanced, s)
    batch_enhanced = d_process_shorts.control(batch_enhanced, s)
    filtered_label_dict = d2_full_enhanced.control(batch_enhanced, s)
    filtered_label_dict = e_twitter_info.control(filtered_label_dict, s)
    label_to_titles = f_titles.control(filtered_label_dict, s)
    label_to_titles = f2_images.control(label_to_titles, s)
    g_produce.control(filtered_label_dict, label_to_titles, s)
    g_produce_prime.control(filtered_label_dict, label_to_titles, s)
    del s
    # del tx
    return None


def control():
    # setting_list = [settings_alan, settings_jd, settings_kv, settings_kaveh, settings_ingrahm, settings_ottolenghi]
    setting_list = [settings_alan, settings_jd, settings_kaveh, settings_ottolenghi, settings_om, settings_jardine, settings_abrams]
    # setting_list = [settings_kaveh, settings_om]
    
    for s in setting_list:
        print("*****************")
        print("Processing Twitter Satellite for " + s["name"])
        print(time.strftime("%H:%M:%S"))
        update_satellite(s)

    return None


class UpdateThread(Thread):
    def __init__(self, callback):
        self.stopped = False
        self.callback = callback
        Thread.__init__(self)  # Call the super construcor (Thread's one)

    def run(self):
        while not self.stopped:
            self.callback()
            time.sleep(60 * 30)


myThread = UpdateThread(control)
myThread.start()
# control()
# quit()
