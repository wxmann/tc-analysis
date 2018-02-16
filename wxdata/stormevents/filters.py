import pandas as pd

import wxdata._timezones as _tzhelp


__all__ = ['filter_on_date', 'filter_region']


def filter_on_date(df, date_, tz=None):
    date_ = pd.Timestamp(date_)
    year, month, day = date_.year, date_.month, date_.day

    t1 = pd.Timestamp(year=year, month=month, day=day)
    if tz is not None:
        t1 = t1.tz_localize(_tzhelp.parse_tz(tz))
    t2 = t1 + pd.Timedelta('1 day')

    return df[(df.begin_date_time >= t1) & (df.begin_date_time < t2)]


def filter_region(df, region_poly):
    return df[df.apply(lambda r: _is_in_region(r, region_poly), axis=1)]


def _is_in_region(event, region_poly):
    from shapely.geometry import Point
    pt = Point(event.begin_lat, event.begin_lon)
    return region_poly.contains(pt)