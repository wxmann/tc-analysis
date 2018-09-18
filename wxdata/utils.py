import os
import shelve
from functools import wraps

import pandas as pd

from wxdata import workdir, _timezones


def diff(df1, df2):
    return df1.merge(df2, indicator=True, how='outer')


# TODO test
def datetime_buckets(start_time, end_time, dt, tz=None):
    start_time = pd.Timestamp(start_time)
    end_time = pd.Timestamp(end_time)
    dt = pd.Timedelta(dt)

    if tz:
        tz = _timezones.parse_tz(tz)
        start_time = start_time.tz_localize(tz)
        end_time = end_time.tz_localize(tz)

    start_bucket = start_time
    while start_bucket < end_time:
        end_bucket = start_bucket + dt
        yield start_bucket, min(end_bucket, end_time)
        start_bucket = end_bucket


def label_iter():
    alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    count = 1
    while True:
        for i in range(len(alphabet)):
            yield alphabet[i] * count
        count += 1


def log_if_debug(stmt, debug):
    if debug:
        print(stmt)


def persistent_cache(saveloc=None, filename='cache', debug=False):

    def decorator(func):
        @wraps(func)
        def wrapped_func(key, *args, **kwargs):
            try:
                saveloc_here = saveloc or workdir.subdir('_cache')
            except workdir.WorkDirectoryException:
                log_if_debug('Cannot find cache location, calling real function...', debug)
                return func(key, *args, **kwargs)

            fullpath = os.path.join(saveloc_here, filename)

            with shelve.open(fullpath) as cache:
                if key in cache:
                    log_if_debug('Fetching from cache for: {}'.format(key), debug)
                    return cache[key]
                else:
                    log_if_debug('Calling real function for: {}'.format(key), debug)
                    ret = func(key, *args, **kwargs)
                    cache[key] = ret
                    return ret

        return wrapped_func

    return decorator


# TODO: this has been moved to the geog module
@persistent_cache(filename='cities')
def find_latlon(loc, geocodor=None):
    if geocodor is None:
        from geopy.geocoders import Nominatim
        geocodor = Nominatim()

    location = geocodor.geocode(loc)

    if not location:
        raise ValueError("Cannot find latlon for {}".format(loc))
    return location.latitude, location.longitude


def wrap_longitude(dataset, lon_dim='lon'):
    londata = dataset[lon_dim]
    if londata.min() < 0.0:
        dataset[lon_dim] = ((londata - 180) % 360) - 180
    return dataset


def two_sided_range(start, stop, step):
    positive_range = list(range(start, stop, step))
    negative_range = reversed([-elem for elem in positive_range if elem != 0])
    return list(negative_range) + positive_range
