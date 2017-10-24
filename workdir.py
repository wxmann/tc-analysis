import os
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from common import saveall

VAR = 'WORKDIR'


class WorkDirectoryException(Exception):
    pass


def get():
    workdir = os.getenv(VAR)
    if not workdir:
        raise WorkDirectoryException('Work directory must be set!')
    if not os.path.isdir(workdir):
        raise WorkDirectoryException('Work directory must be a valid directory!')
    return workdir


def filename_from(url):
    relpath = urlparse(url)
    return os.path.basename(relpath.path)


def bulksave(urls, override_existing=False, postsave=None):
    workdir = get()
    no_file = ''

    def dest(url):
        filename = filename_from(url)
        if filename and '.' in filename:
            return os.path.join(workdir, filename)
        else:
            return no_file

    src_dest_map = {url: dest(url) for url in urls if dest(url) != no_file}
    responses = saveall(src_dest_map, override_existing, postsave)

    successes = {}
    for resp in responses:
        if resp is not None:
            # HACK: if the file already exists, assume it's okay regardless of outcome
            # of HTTP request
            if os.path.isfile(src_dest_map[resp.url]):
                successes[resp.url] = src_dest_map[resp.url]

    errors = {k: v for k, v in src_dest_map.items() if k not in successes}
    return successes, errors