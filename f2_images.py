import utils
import base64
import requests
import PIL.Image
from io import BytesIO

#IMPORTANT to prevent unrecoverable "decompression bomb" errors in PIL
PIL.Image.MAX_IMAGE_PIXELS = 4000*4000

def make_image_key(url):
    try:
        urlx = bytes(url, 'utf8')
        enc = base64.b64encode(urlx)
        write_name = enc.decode().replace("/","-")

    except:
        write_name = None
    return write_name
    
def modify_image(pil_image, f):
    try:
        in_mem_file = BytesIO()
        pil_image.save(in_mem_file, format=f)
        val = in_mem_file.getvalue()
        in_mem_file.close()
        return val
    except Exception as e: 
        print(e)
        return None

def crop_image(im):
    width, height = im.size   # Get dimensions
    s = width if width <= height else height
    left = (width - s)/2
    top = (height - s)/2
    right = (width + s)/2
    bottom = (height + s)/2
    cropped = im.crop((left, top, right, bottom))
    return cropped

def make_image_square(iurl):
    try:
        headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.1 Safari/605.1.15'}
        r = requests.get(iurl, headers=headers, timeout=2) 
        if r.status_code != 200:
            print("Image Status code " + str(r.status_code))
            return None, None, None
        data = r.content
        im = PIL.Image.open(BytesIO(data)) 
        size = im.size
        ft = im.format
        print(ft)
        thumb = crop_image(im)
        return thumb, size, ft
    except Exception as e: 
        print(e)
        return None, None, None
    
def writeImageToS3(im_thumb, format, source_url):
    try:
        writable_image = modify_image(im_thumb, format)
        write_dir = "data-aws/shared_data/images" + utils.file_date() + "/"
        # write_dir = s["s3dir"] + 'production/images/'+utils.file_date()+'/'
        write_name = make_image_key(source_url) + "." + format
        print(write_name)
        utils.write_binary_to_s3(writable_image, write_name, public=True, directory=write_dir)
        display_url = 'https://s3.amazonaws.com/twitter-satellite/' + write_dir + write_name
    except Exception as e: 
        print(e)
        print("error making image for " + source_url)
        display_url = None
    return display_url

def errorReturn(top_image):
    print("problem with " + top_image)
    row = {
        "thumb_fetch_successful": False,
        "media_fetch_successful": True,
    }
    return row

def make_thumb(top_image):
    print("cache miss for " + top_image)
    thumb_size = 100,100
    f_base = {}
    try:
        im, size, ft = make_image_square(top_image)
        print("made size")
        if im == None:
            return errorReturn(top_image)
        im_thumb = im.resize(thumb_size)
        print(im_thumb.size)
        display_url = writeImageToS3(im_thumb, ft, top_image)
        if display_url == None:
            return errorReturn(top_image)
        f_base = {
            'media_url_news': top_image,
            "media_url_news_size": [size[0],size[1]],
            "media_url_news_thumb": display_url,
            "media_url_news_thumb_size": [im_thumb.size[0], im_thumb.size[1]],
            "thumb_fetch_successful": True,
            "media_fetch_successful": True,
        }
    except Exception as e: 
        print(e)
        return errorReturn(top_image)
    utils.add_to_cache(top_image, f_base, "thumbs", ex=432800)
    return f_base

def add_image_thumbs( titles, s):
    need_thumbs = [key for key, item in titles.items() if "news_image" in item and 'media_url_news_thumb' not in item["news_image"] and item["news_image"]["media_fetch_successful"] == True]

    print("thumbs " + str(len(need_thumbs)))
    for url in need_thumbs:
        # if "thumb_fetch_successful" not in titles[url]["news_image"] and 'media_url_news' in titles[url]["news_image"]:
        if 'media_url_news' in titles[url]["news_image"]:
            top_image = titles[url]["news_image"]['media_url_news']
            print("Need image for " + top_image + ", " + url)
            ## CACHE CHECKING
            f_base = utils.get_from_cache(top_image, "thumbs", print_hits=True)
            if f_base == None:
                f_base = make_thumb(top_image) 
            titles[url]["news_image"] = f_base
        else:
            print("already tried fetching thumb for " + url)
    return titles

def control(label_to_titles, s):
    utils.log(str(len(label_to_titles)) + " titles loaded")
    label_to_titles = add_image_thumbs(label_to_titles, s)

    utils.write_to_s3(
        label_to_titles, 'batch_titles_fd_' + utils.file_date() + '.json', directory=s["s3dir"]
    )
    return label_to_titles

if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    label_to_titles = utils.read_from_s3('batch_titles_fd_' + utils.file_date() + '.json', directory=sd["s3dir"])
    control(label_to_titles, sd)
