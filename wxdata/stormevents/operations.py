import pandas as pd

import wxdata._timezones as _tzhelp
from wxdata.stormevents.temporal import df_tz

__all__ = ['filter_on_date', 'filter_on_year', 'filter_region', 'time_partition']


def time_partition(df, timebuckets):
    for bucket in timebuckets:
        bucket_start, bucket_end = bucket
        yield bucket, df[(df.begin_date_time >= bucket_start) &
                         (df.begin_date_time < bucket_end)]


def filter_on_date(df, date_, tz_localize=True, tz=None):
    date_ = pd.Timestamp(date_)
    year, month, day = date_.year, date_.month, date_.day

    t1 = pd.Timestamp(year=year, month=month, day=day)
    return _filter_on_timeperiod(df, t1, '1 day', tz_localize, tz)


def filter_on_year(df, year, tz_localize=True, tz=None):
    t1 = pd.Timestamp(year=year, month=1, day=1, hour=0, minute=0)
    return _filter_on_timeperiod(df, t1, '1 year', tz_localize, tz)


def _filter_on_timeperiod(df, t1, dt, tz_localize, tz):
    if df.empty:
        return df

    if tz_localize:
        if tz is None:
            tz = df_tz(df)
        t1 = t1.tz_localize(_tzhelp.parse_tz(tz))

    dt = pd.Timedelta(dt)
    t2 = t1 + dt
    return df[(df.begin_date_time >= t1) & (df.begin_date_time < t2)]


def filter_region(df, region_poly):
    return df[df.apply(lambda r: _is_in_region(r, region_poly), axis=1)]


def _is_in_region(event, region_poly):
    from shapely.geometry import Point
    pt = Point(event.begin_lat, event.begin_lon)
    return region_poly.contains(pt)