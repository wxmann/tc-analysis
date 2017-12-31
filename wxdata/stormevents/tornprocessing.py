import pandas as pd
import numpy as np

from wxdata import _timezones as _tz
from wxdata.stormevents.temporal import sync_datetime_fields, localize_timestamp_tz

__all__ = ['longevity', 'ef', 'speed_mph', 'correct_tornado_times', 'discretize']

_ONE_DAY = pd.Timedelta(days=1)
_ONE_HOUR = pd.Timedelta(hours=1)
_ONE_MINUTE = pd.Timedelta(minutes=1)
_TORNADO_LONVEVITY_LIMIT = pd.Timedelta(hours=4)


def longevity(df):
    return df.end_date_time - df.begin_date_time


def ef(df):
    frating = df.tor_f_scale.str.replace(r'\D', '')
    return pd.to_numeric(frating, errors='coerce')


def speed_mph(df, floor_longevity=None):
    if floor_longevity is None:
        floor_longevity = pd.Timedelta(seconds=30)

    longevities = longevity(df)
    longevities[longevities < floor_longevity] = np.nan
    longevities = pd.to_timedelta(longevities)
    longevities /= pd.Timedelta('1 hour')
    path_lens = df['tor_length']

    return path_lens / longevities


def correct_tornado_times(df, copy=True):
    if copy:
        df = df.copy()
    vals = df[['event_type', 'begin_date_time', 'end_date_time', 'tor_length']].values

    df[['begin_date_time', 'end_date_time']] = np.apply_along_axis(_corrected_times_for,
                                                                   axis=1, arr=vals)
    return sync_datetime_fields(df)


def _corrected_times_for(torn, indices=None):
    mandatory = ('event_type', 'begin_date_time', 'end_date_time', 'tor_length')
    if indices is None:
        indices = {col: index for index, col in enumerate(mandatory)}

    assert all(col in indices for col in mandatory)

    end_date_time = torn[indices['end_date_time']]
    begin_date_time = torn[indices['begin_date_time']]
    torlen = torn[indices['tor_length']]

    if torn[indices['event_type']] != 'Tornado':
        return begin_date_time, end_date_time

    def _correct_only_end(begin, end):
        elapsed = end - begin

        if elapsed >= _TORNADO_LONVEVITY_LIMIT:
            # the longest-lived tornado as of 2017 is the tri-state tornado (3.5 hr)
            # anything greater than 4 is certainly suspicious.
            if torlen < 0.3:
                # short-lived tornado, we assume brief touchdown
                return begin
            elif end >= begin + _ONE_DAY:
                # if possibly not a brief touchdown, assume end-time was entered with wrong day.
                return elapsed % _ONE_DAY + begin
            else:
                # fall back to brief touchdown if no other information
                return begin
        elif elapsed >= _ONE_HOUR:
            mph = torlen / (elapsed.seconds / 3600)
            # assume off-by-one error in hour if tornado is moving erroneously slowly
            if mph < 8:
                hours = elapsed.seconds // 3600
                return elapsed % _ONE_HOUR + pd.Timedelta(hours - 1) + begin
        # don't correct
        return end

    if end_date_time >= begin_date_time:
        result_begin, result_end = begin_date_time, _correct_only_end(begin_date_time, end_date_time)
    else:
        elapsed_rev = begin_date_time - end_date_time
        if elapsed_rev < _TORNADO_LONVEVITY_LIMIT:
            # assume times were accidentally swapped in entries
            new_end_time, new_begin_time = begin_date_time, end_date_time
            result_begin, result_end = new_begin_time, _correct_only_end(new_begin_time, new_end_time)
        else:
            # off-by-one error in date entry
            result_begin, result_end = begin_date_time, end_date_time + _ONE_DAY

    return [result_begin, result_end]


def discretize(df, spacing_min=1):
    foreachtor = [discretize_tor(tor, spacing_min) for _, tor in df.iterrows()]
    return pd.concat(foreachtor, ignore_index=True)


def discretize_tor(torn_seg, spacing_min=1):
    elapsed_min = (torn_seg.end_date_time - torn_seg.begin_date_time) / pd.Timedelta('1 min')
    tzstr = torn_seg.cz_timezone
    slat, slon, elat, elon = torn_seg.begin_lat, torn_seg.begin_lon, torn_seg.end_lat, torn_seg.end_lon
    numpoints = elapsed_min // spacing_min

    if numpoints == 0:
        numpoints = 1

    lat_space = np.linspace(slat, elat, numpoints, endpoint=False)
    lon_space = np.linspace(slon, elon, numpoints, endpoint=False)
    latlons = np.vstack([lat_space, lon_space]).T

    t0 = torn_seg.begin_date_time
    t1 = torn_seg.end_date_time

    t0 = localize_timestamp_tz(t0, tzstr)
    t1 = localize_timestamp_tz(t1, tzstr)

    time_space = np.linspace(t0.value, t1.value, numpoints, endpoint=False)
    times = pd.to_datetime(time_space)

    ret = pd.DataFrame(latlons, columns=['lat', 'lon'])
    ret['event_id'] = torn_seg.event_id
    ret['timestamp'] = times.tz_localize('GMT').tz_convert(_tz.parse_tz(tzstr))

    return ret
