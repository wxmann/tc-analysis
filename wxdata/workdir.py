import os
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from wxdata.common import saveall

VAR = 'WORKDIR'


class WorkDirectoryException(Exception):
    pass


def get():
    workdir = os.getenv(VAR)
    if not workdir:
        raise WorkDirectoryException('Work directory must be set!')

    if workdir[0] == '~':
        workdir = os.path.expanduser(workdir)

    if not os.path.isdir(workdir):
        raise WorkDirectoryException('Work directory must be a valid directory!')

    return workdir


def setto(directory):
    os.environ[VAR] = directory


def save_dest(url):
    workdir = get()
    relpath = urlparse(url)
    filename = os.path.basename(relpath.path)

    if filename and '.' in filename:
        return os.path.join(workdir, filename)
    else:
        return ''


def bulksave(urls, override_existing=False, postsave=None):
    src_dest_map = {}
    for url in urls:
        dest = save_dest(url)
        if dest:
            src_dest_map[url] = dest

    return saveall(src_dest_map, override_existing, postsave)
