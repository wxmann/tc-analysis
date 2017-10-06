from collections import namedtuple

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


def iter_text_lines(url, skip_empty=True):
    resp = requests.get(url, stream=True)
    raise_for_bad_response(resp)

    line_iter = resp.iter_lines(decode_unicode=True)

    if skip_empty:
        return (line for line in line_iter if line)
    else:
        return line_iter


def raise_for_bad_response(resp, expected_status_code=200):
    if resp is None or resp.status_code != expected_status_code:
        raise DataRetrievalException("Could not find data for url: {}".format(resp.url))


def column_definition(def_id, columns_list):
    columndef = namedtuple(def_id, columns_list)
    return columndef(**{col: col for col in columns_list})