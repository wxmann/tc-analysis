import os
import shutil
from collections import namedtuple
from functools import partial
from multiprocessing import Pool

import requests
from bs4 import BeautifulSoup


class DataRetrievalException(Exception):
    pass


def get_links(url, ext=None, file_filter=None):
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
    results = (url + '/' + node.get('href') for node in parser.find_all('a')
               if node.get('href').endswith(ext))

    if file_filter is not None:
        return [result for result in results if file_filter(result)]
    else:
        return list(results)


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


class _SaveResult(object):
    def __init__(self, url, dest, response, executed_request, expected_status=200):
        self.url = url
        self.dest = dest
        self.response = response
        self.exceptions = []
        self.output = None
        self._executed_request = executed_request
        self._expected_status = expected_status

    @property
    def success(self):
        if self._executed_request:
            http_success = self.response is not None and self.response.status_code == self._expected_status
        else:
            http_success = True

        return http_success and not self.exceptions

    def __str__(self):
        if self.response is not None:
            response_str = self.response.status_code
        elif not self._executed_request:
            response_str = 'No HTTP call executed'
        else:
            response_str = 'No response'
        return '<URL: {}, Response: {}, Callback exceptions: {}>'.format(self.url, response_str,
                                                                         len(self.exceptions))


def saveall(src_dest_map, override_existing=False, callback=None):
    save_and_exec = partial(_save_and_exec_callback, src_dest_map=src_dest_map,
                            override_existing=override_existing, callback=callback)
    with Pool(4) as pool:
        return pool.map(save_and_exec, src_dest_map.keys())


def _save_and_exec_callback(url, src_dest_map, override_existing, callback):
    def wrapped_callback(arg, save_result):
        if callback is not None:
            try:
                # print("arrived at callback for: {}".format(arg))
                ret = callback(arg)
                save_result.output = ret
            except Exception as e:
                save_result.exceptions.append(e)

    dest = src_dest_map[url]
    if not override_existing and os.path.isfile(dest):
        result = _SaveResult(url, dest, response=None, executed_request=False)
        wrapped_callback(dest, result)
        return result
    else:
        response = requests.get(url, stream=True)
        result = _SaveResult(url, dest, response, executed_request=True)

        if response is not None:
            try:
                response.raise_for_status()
                save_response_content(response, dest)
                wrapped_callback(dest, result)
            except requests.HTTPError as e:
                result.exceptions.append(e)
        return result
