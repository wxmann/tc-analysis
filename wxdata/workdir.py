import os

import errno

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from wxdata.http import saveall

VAR = 'WORKDIR'


class WorkDirectoryException(Exception):
    pass


def get():
    workdir = os.getenv(VAR)
    if not workdir:
        raise WorkDirectoryException('Work directory must be set!')

    workdir = os.path.expanduser(workdir)

    if not os.path.isdir(workdir):
        raise WorkDirectoryException('Work directory: {} must be a valid directory!'.format(workdir))

    return workdir


def subdir(name):
    path = os.path.join(get(), name)
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise WorkDirectoryException(e)
    return path


def setto(directory):
    os.environ[VAR] = directory


def save_dest(url, in_subdir=None):
    if in_subdir is not None:
        savedir = subdir(in_subdir)
    else:
        savedir = get()

    relpath = urlparse(url)
    filename = os.path.basename(relpath.path)

    if filename and '.' in filename:
        return os.path.join(savedir, filename)
    else:
        return ''


def bulksave(urls, in_subdir=None, override_existing=False, postsave=None):
    src_dest_map = {}
    for url in urls:
        dest = save_dest(url, in_subdir)
        if dest:
            src_dest_map[url] = dest

    return saveall(src_dest_map, override_existing, postsave)
