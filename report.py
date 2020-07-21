# This program generate simple visitors-report from Yandex Metrika 
import requests
import configparser
import json
import pandas as pd
import os

SCRIPT_URL = os.path.dirname(os.path.abspath(__file__))
REQUEST_PREFIX = "https://api-metrika.yandex.net/stat/v1/data"

# request fields
IDS = "?ids="
METRICS = "&metrics=" 
FILTERS = "&filters="
METRIC_PAGEVIEWS = "ym:s:pageviews"
FROM = "&date1="
TO = "&date2="

# report period
date1 = "2020-07-01"
date2 = "2020-07-21"

# config import
config = configparser.ConfigParser()
config.read('settings.ini')
token = 'OAuth ' + config['Metrika']['metrika_token']
counter = config['Metrika']['counter']

def construct_url(url):
    metric_url = "EXISTS(ym:pv:URL=*'{}')".format(url)
    return REQUEST_PREFIX + IDS + counter + METRICS + METRIC_PAGEVIEWS + FILTERS + metric_url + FROM + date1 + TO + date2

def make_request(url):
    headers = {
        'Authorization': token
    }
    test_request = construct_url(url)
    response = requests.get(test_request, headers = headers)
    response_json = response.text
    return response_json

def get_page_views(url):
    response = make_request(url)
    parsed_response = json.loads(response)
    return int(parsed_response["data"][0]["metrics"][0])

def get_list_of_url(filename):
    columns = 'url'
    data = pd.read_csv('{}/{}'.format(SCRIPT_URL, filename), header=None, names=[columns])
    urls = []
    for i in range(0, len(data)):
        urls.append(data.iloc[i][columns])
    return urls

urls = get_list_of_url("test.csv")
results = []
for url in urls:
    results.append(get_page_views(url))
ready_dict = {'urls': urls, 'results': results}

result_dataframe = pd.DataFrame.from_dict(ready_dict)

# export to csv
export_filename  = '{}/report.csv'.format(SCRIPT_URL)
result_dataframe.to_csv(export_filename, sep=';', columns=['urls', 'results'], index = False, encoding='utf-8', quotechar='"')
print("done")
