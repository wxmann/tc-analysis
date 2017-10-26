import os
import shutil
from collections import namedtuple

import grequests
import requests
from bs4 import BeautifulSoup


class DataRetrievalException(Exception):
    pass


def get_links(url, ext=None):
    if not url:
        raise ValueError("Must enter a non-empty url")
    if url[-1] == '/':
        url = url[:-1]
    if ext is None:
        ext = ''

    resp = requests.get(url)
    raise_for_bad_response(resp)

    page_text = resp.text
    parser = BeautifulSoup(page_text, 'html.parser')
    return [url + '/' + node.get('href') for node in parser.find_all('a')
            if node.get('href').endswith(ext)]


# TODO: convert this to iterator over mutliple urls
def iter_text_lines(url, skip_empty=True):
    resp = requests.get(url, stream=True)
    raise_for_bad_response(resp)

    line_iter = resp.iter_lines(decode_unicode=True)

    if skip_empty:
        return (line for line in line_iter if line)
    else:
        return line_iter


def raise_for_bad_response(resp, expected_status_code=200):
    if resp is None:
        raise DataRetrievalException("Could not find data for one the responses")
    if resp.status_code != expected_status_code:
        raise DataRetrievalException("Could not find data for url: {}".format(resp.url))


def column_definition(def_id, columns_list):
    columndef = namedtuple(def_id, columns_list)
    return columndef(**{col: col for col in columns_list})


# TODO: get rid of singular save_from_rul
def save_from_url(url, saveloc):
    response = requests.get(url, stream=True)
    raise_for_bad_response(response)

    with open(saveloc, 'wb') as f:
        response.raw.decode_content = True
        shutil.copyfileobj(response.raw, f)


def save_response_content(response, saveloc):
    raise_for_bad_response(response)

    with open(saveloc, 'wb') as f:
        response.raw.decode_content = True
        shutil.copyfileobj(response.raw, f)


def saveall(src_dest_map, override_existing=False, callback=None):
    def save_and_exec_callback(response, **kwargs):
        if response is not None:
            dest = src_dest_map[response.url]
            if override_existing or not os.path.isfile(dest):
                save_response_content(response, dest)
            if callback is not None and os.path.isfile(dest):
                callback(dest)

    reqs = (grequests.get(src_url, hooks=dict(response=save_and_exec_callback)) for src_url
            in src_dest_map)
    return grequests.map(reqs, stream=True, size=6)
