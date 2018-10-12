import re
import utils
from urllib.parse import urlparse
from newspaper import Article, Config
import pickle

ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.1 Safari/605.1.15'


def get_title_urls(labels_raw, label_to_titles):
    labels = [l for l in labels_raw]
    urls = [
        l for l in labels
        if re.findall("^https://.*|^http://.*", l) and l not in label_to_titles
    ]
    utils.log(len(urls), "URLs to process: ")
    return urls

def title_from_html(html):
    title_clean = None
    if html:
        title2 = re.search('<title[^>]*>([^<]+)</title>', html)
        if title2 != None and title2.group(1) != None:
            title_clean = clean_title(title2.group(1))
    return title_clean

def clean_title(tstr):
    if tstr and isinstance(tstr, str):
        tstr = re.sub("\n", "", tstr)
        tstr = re.sub("\s{2,}", "", tstr)
    return tstr

def get_domain_from_url(url):
    parsed_uri = urlparse(url)
    domain = parsed_uri.netloc
    domain_parts = domain.split(".")
    return_val = domain_parts[-2] if len(domain_parts) >=2 else ""
    for candidate in domain_parts:
        if len(candidate) > len (return_val):
            return_val = candidate
    return return_val


def add_domain(url, t):
    parsed_uri = urlparse(url)
    domain = parsed_uri.netloc
    domain_main = domain.split(".")[-2]
    if t != None:
        x = re.findall("(.*)\s\s*[|–:\-»—]\s\s*(.*)", t)
        if x and len(x[0]) == 2:
            test = []
            candidate_pub = (x[0][-1]).lower().replace(" ", "")
            for c in domain_main:
                in_candidate = c in candidate_pub
                test.append(in_candidate)
            if sum(test) / len(test) > .8 and len(x[0][-1]) < 35:
                domain_main = x[0][-1]
    return domain_main


def get_site_name(article, url):
    site_name = None
    md = article.meta_data
    if "og" in md:
        if "site_name" in md["og"]:
            site_name = md["og"]["site_name"]
        elif utils.key_exists(["al","iphone", "app_name"], md):
            site_name = article.meta_data["al"]["iphone"]["app_name"]
    if site_name == None:
        title_from_tag = title_from_html(article.html)
        site_name = add_domain(url, title_from_tag)
    return site_name


def get_title_data(url):
        print("Title cache miss for " + url)
        split_result = url.rsplit(".", 1)
        tail = split_result[1]
        if tail.lower() in ["pdf"]:
            print("Article can't parse a PDF. ")
            print("Skipping title and thumb for " + url)
            domain = get_domain_from_url(url)
            row = {
                'news_image': {'media_fetch_successful': False},
                'domain': domain,
            }
        elif url.find("bloomberg.com") > -1:
            print("Skipping Bloomberg articles for now ")
            print("Skipping title and thumb for " + url)
            domain = get_domain_from_url(url)
            row = {
                'news_image': {'media_fetch_successful': False},
                'domain': domain,
            }
        else:
            try:
                config = Config()
                config.browser_user_agent = ua
                article = Article(url, config)
                article.download()
                article.parse()
                ## Article sometimes returns weird classes instead of strings
                if type(article.title) != str:
                    row = {
                        'news_image': {'media_fetch_successful': False},
                        'domain': domain,
                    }
                else:
                    site_name = get_site_name(article, url)
                    row = {
                        'domainless_title': article.title,
                        'parsed_title': article.title,
                        'domain': site_name,
                        'news_image': {'media_fetch_successful': True}
                    }

                    if article.top_image and article.top_image != None:
                        tail = article.top_image.split(".")[-1]
                        if tail != 'ico':
                            row["news_image"]['media_url_news'] = article.top_image
            except:
                print('problem with ')
                domain = get_domain_from_url(url)
                row = {
                    'news_image': {'media_fetch_successful': False},
                    'domain': domain,
                }
        ## CACHE

        utils.add_to_cache(url, row, "titles", ex=432800)
        ## CACHE
        return row

def process_urls_new(url_list):
    new_titles = {}
    for url in url_list:
        print("Need Title for " + url)
        ## CACHE
        row = utils.get_from_cache(url, "titles", print_hits=True)
        ## CACHE
        if row == None:
            row = get_title_data(url)
        new_titles[url] = row
    return new_titles

def control(filt_label_dict, s):
    label_to_titles = utils.read_from_s3(
        'batch_titles_fd_' + utils.file_date() + '.json', directory=s["s3dir"])
    utils.log(len(filt_label_dict), "Labels loaded: ")
    utils.log(len(label_to_titles), "Titles loaded: ")
    url_list = get_title_urls(filt_label_dict, label_to_titles)
    new_titles = process_urls_new(url_list)
    label_to_titles.update(new_titles) 
    # utils.write_to_s3(label_to_titles, 'titles_fd_' + utils.file_date() + '.json', directory=s["s3dir"])
    utils.log(len(label_to_titles), "Titles returned: ")
    return label_to_titles

if __name__ == "__main__":
    sd = utils.getDefaultSettings()
    filt_label_dict = utils.read_from_s3(
        utils.file_name(prefix='_batch_filt_label_dict_enhanced_'),
        directory=sd["s3dir"])
    control(filt_label_dict, sd)