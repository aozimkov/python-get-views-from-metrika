# This program generate simple visitors-report from Yandex Metrika 
import requests
import configparser
import json
import pandas as pd
import os
from urllib2 import unquote
from urlparse import urlparse

SCRIPT_URL = os.path.dirname(os.path.abspath(__file__))
REQUEST_PREFIX = "https://api-metrika.yandex.net/stat/v1/data"
PROJECT_NAME = "ExampleProject"

# request fields
IDS = "?ids="
LIMITS = "&limit=" 
LIMIT  = "100000"
METRICS = "&metrics="
METRIC_PAGEVIEWS = "ym:pv:pageviews"
DIMENSIONS = "&dimensions="
DIMENSION_URL = "ym:pv:URL"
FROM = "&date1="
# TO = "&date2="

# report period
date1 = "2020-06-01"
# date2 = "2020-07-31"

# config import
config = configparser.ConfigParser()
config.read('settings.ini')
token = 'OAuth ' + config[PROJECT_NAME]['metrika_token']
counter = config[PROJECT_NAME]['counter']
links_filename = config[PROJECT_NAME]['filename']

def construct_url():
    return REQUEST_PREFIX + IDS + counter + LIMITS + LIMIT + METRICS + METRIC_PAGEVIEWS + DIMENSIONS + DIMENSION_URL + FROM + date1 # + TO + date2

def make_request():
    status_code = 0
    while status_code != 200:
        headers = {
            'Authorization': token
        }
        request_metrics = construct_url()
        response = requests.get(request_metrics, headers = headers)
        response_json = response.content
        status_code = response.status_code
    return response_json

# get url and return it without any params
def clean_params(url):
    url = urlparse(url)
    return url.scheme + "://" + url.netloc + url.path

def get_urls_pageviews_dict(data):
    data_object = data['data']
    url_pageviews_dict = {}
    for i in range(len(data_object)):
        url = clean_params(data_object[i]['dimensions'][0]['name']).encode('utf-8')
        visitors = data_object[i]['metrics'][0]
        if url in url_pageviews_dict:
            url_pageviews_dict[url] += visitors
        else:
            url_pageviews_dict[url] = visitors
    return url_pageviews_dict

# Method return json object with the information about visitors from ya metrika
def get_all_views():
    response = make_request()
    ready_data_object = json.loads(response)
    return get_urls_pageviews_dict(ready_data_object)

# get file by file name and return urls array
def get_list_of_url(filename):
    columns = 'url'
    data = pd.read_csv('{}/{}'.format(SCRIPT_URL, filename), header=None, names=[columns])
    data.drop_duplicates(subset=columns, keep=False, inplace=True)
    urls = []
    for i in range(0, len(data)):
        url = data.iloc[i][columns]
        url = unquote(url)
        urls.append(url)
    return urls

def main():
    print("Loading data form Metrika ...")
    all_views_dict = get_all_views()
    print("{} objects loaded".format(len(all_views_dict)))
    
    print("Loading urls from file...")
    urls = get_list_of_url(links_filename)
    print("{} urls loaded".format(len(urls)))

    print("Processing urls ...")
    results = []
    for url in urls:
        results.append(int(all_views_dict.get(url, 0)))
    ready_dict = {'urls': urls, 'results': results}
    print("{} urls processed".format(len(results)))

    result_dataframe = pd.DataFrame.from_dict(ready_dict)

    # export to csv
    export_filename  = '{}/report-{}'.format(SCRIPT_URL, links_filename)
    print("Exporting to './report-{} ...".format(links_filename))
    result_dataframe.to_csv(export_filename, sep=';', columns=['urls', 'results'], index = False, encoding='utf-8', quotechar='"')
    print("Already done!")

if __name__ == "__main__":
    main()