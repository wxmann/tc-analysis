from __future__ import division

import re
import warnings
from datetime import datetime
from functools import partial
from itertools import product

import numpy as np
import pandas as pd
import six

from wxdata import _timezones as _tzhelp
from wxdata.common import get_links, DataRetrievalException
from wxdata.workdir import bulksave


def urls_for(years):
    return get_links('https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/',
                     file_filter=_year_filter(years))


def _year_filter(years):
    years = (str(yr) for yr in years)
    regex = r'StormEvents_details-ftp_v\d{1}\.\d{1}_d({YEARS})_c\d{8}.csv.gz'.replace(r'{YEARS}', '|'.join(years))

    def ret(link):
        return bool(re.search(regex, link))

    return ret


def _year_from_link(link):
    regex = r'StormEvents_details-ftp_v\d{1}\.\d{1}_d(\d{4})_c\d{8}.csv.gz'
    matches = re.search(regex, link)
    if matches:
        year = matches.group(1)
        return int(year)
    else:
        raise DataRetrievalException("Could not get year from assumed storm event "
                                     "CSV link: {}".format(link))


def load_file(file, keep_data_start=None, keep_data_end=None,
              eventtypes=None, states=None, tz=None):
    df = pd.read_csv(file,
                     parse_dates=['BEGIN_DATE_TIME', 'END_DATE_TIME'],
                     infer_datetime_format=True,
                     index_col=False,
                     converters={
                         'BEGIN_TIME': lambda t: t.zfill(4),
                         'END_TIME': lambda t: t.zfill(4)
                     },
                     dtype={'{}_{}'.format(flag, temporal_accessor): object
                            for flag, temporal_accessor
                            in product(('BEGIN', 'END'), ('YEARMONTH', 'TIME'))},
                     compression='infer')

    df.columns = map(str.lower, df.columns)

    if eventtypes is not None:
        df = df[df.event_type.isin(eventtypes)]
    if states is not None:
        df = df[df.state.isin([state.upper() for state in states])]

    if tz:
        df = convert_df_tz(df, tz, False)
    if keep_data_start and keep_data_end:
        df = df[(df.begin_date_time >= keep_data_start) & (df.begin_date_time < keep_data_end)]

    return df


def load_events(start, end, eventtypes=None, states=None, tz=None, debug=False):
    if isinstance(start, six.string_types):
        start = pd.Timestamp(start)
    if isinstance(end, six.string_types):
        end = pd.Timestamp(end)

    if end < start:
        raise ValueError("End date must be on or after start date")
    year1 = start.year
    year2 = end.year
    links = urls_for(range(year1, year2 + 1))

    load_df_with_filter = partial(load_file, keep_data_start=None, keep_data_end=None,
                                  eventtypes=eventtypes, states=states, tz=tz)

    results = bulksave(links, postsave=load_df_with_filter)
    dfs = [result.output for result in results if result.success and result.output is not None]
    errors = [result for result in results if not result.success]

    if errors:
        err_yrs = [str(_year_from_link(err.url)) for err in errors]
        warnings.warn('There were errors trying to load dataframes for years: {}'.format(','.join(err_yrs)))

    if dfs:
        ret = pd.concat(dfs, ignore_index=True)
        ret = ret[(ret.begin_date_time >= start) & (ret.begin_date_time < end)]
        ret.reset_index(drop=True, inplace=True)
    else:
        ret = pd.DataFrame()

    if debug:
        return results, ret
    else:
        return ret


def load_events_year(year, **kwargs):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31, 23, 59, 59)
    return load_events(start, end, **kwargs)


tornadoes = partial(load_events, eventtypes=['Tornado'])
hail = partial(load_events, eventtypes=['Hail'])
tstorm_wind = partial(load_events, eventtypes=['Thunderstorm Wind'])
all_severe = partial(load_events, eventtypes=['Tornado', 'Hail', 'Thunderstorm Wind'])


### TIMEZONE UTILITIES ###


def convert_df_tz(df, to_tz='CST', copy=True):
    assert all([
        'state' in df.columns,
        'cz_timezone' in df.columns,
        'begin_date_time' in df.columns
    ])
    if copy:
        df = df.copy()

    for col in ('begin_date_time', 'end_date_time'):
        if col in df.columns:
            df[col] = df.apply(lambda row: convert_row_tz(row, col, to_tz), axis=1)

    return _sync_datetime_fields(df, to_tz)


def convert_row_tz(row, col, to_tz):
    try:
        state = row['state']
        # In older versions of storm events, `AST` and `AKST` are both logged as `AST`.
        # We can't let our tz-conversion logic believe naively it's Atlantic Standard Time
        if row['cz_timezone'] == 'AST':
            if state == 'ALASKA':
                return convert_timestamp_tz(row[col], 'AKST-9', to_tz)
            else:
                # both Puerto Rico and Virgin Islands in AST
                return convert_timestamp_tz(row[col], 'AST-4', to_tz)

        # moving on now...
        return convert_timestamp_tz(row[col], row['cz_timezone'], to_tz)
    except ValueError as e:
        # warnings.warn("Encountered an error while converting time zone, {}. "
        #               "\nAttempting to use location data to determine tz".format(repr(e)))
        try:
            tz = _tzhelp.tz_for_state(row.state)
            if tz:
                return tz.localize(row[col]).tz_convert(_pdtz_from_str(to_tz))
            else:
                lat, lon = row['begin_lat'], row['begin_lon']
                tz = _tzhelp.tz_for_latlon(lat, lon)

                # have to do some fudging here since parsed TZ from the geopy module
                # is DST-dependent. Our data is frozen onto one TZ.
                hour_offset = _tzhelp.utc_offset_no_dst(tz)
                if hour_offset < 0:
                    return convert_timestamp_tz(row[col], 'Etc/GMT+{}'.format(-hour_offset), to_tz)
                else:
                    return convert_timestamp_tz(row[col], 'Etc/GMT-{}'.format(hour_offset), to_tz)
        except KeyError as innere:
            # FIXME: KeyError is a superclass of UnknownTimeZoneError... wtf!!! Somehow handle this.
            print(repr(innere))
            warnings.warn("Can't find timezone with missing lat lon or state data.")

        raise e
    except KeyError:
        raise ValueError("Row must have `state` and `cz_timezone` column")


def convert_timestamp_tz(timestamp, from_tz, to_tz):
    original_tz_pd = _pdtz_from_str(from_tz)
    new_tz_pd = _pdtz_from_str(to_tz)
    return pd.Timestamp(timestamp, tz=original_tz_pd).tz_convert(new_tz_pd)


def _pdtz_from_str(tz_str):
    if not tz_str or tz_str in ('UTC', 'GMT'):
        import pytz
        return pytz.timezone('GMT')

    tz_str_up = tz_str.upper()
    # handle the weird cases
    if tz_str_up in ('SCT', 'CSC'):
        # found these egregious typos
        raise ValueError("{} is probably CST but cannot determine for sure".format(tz_str))
    elif tz_str_up == 'UNK':
        raise ValueError("UNK timezone")

    # we're safe; fallback to our usual timezone parsing logic
    return _tzhelp.parse_tz(tz_str)


### TORNADOES ###


class _Torn(object):
    _ONE_DAY = pd.Timedelta(days=1)
    _ONE_HOUR = pd.Timedelta(hours=1)
    _ONE_MINUTE = pd.Timedelta(minutes=1)
    _TORNADO_LONVEVITY_LIMIT = pd.Timedelta(hours=4)

    def __init__(self):
        pass

    @staticmethod
    def longevity(df):
        return df.end_date_time - df.begin_date_time

    @staticmethod
    def ef(df):
        frating = df.tor_f_scale.str.replace(r'\D', '')
        return pd.to_numeric(frating, errors='coerce')

    @staticmethod
    def speed_mph(df, floor_longevity=None):
        if floor_longevity is None:
            floor_longevity = pd.Timedelta(seconds=30)

        longevities = _Torn.longevity(df)
        longevities[longevities < floor_longevity] = np.nan
        longevities = pd.to_timedelta(longevities)
        longevities /= pd.Timedelta('1 hour')
        path_lens = df['tor_length']

        return path_lens / longevities

    @staticmethod
    def correct_tornado_times(df, copy=True):
        if copy:
            df = df.copy()
        vals = df[['event_type', 'begin_date_time', 'end_date_time', 'tor_length']].values

        df[['begin_date_time', 'end_date_time']] = np.apply_along_axis(_Torn._corrected_times_for,
                                                                       axis=1, arr=vals)
        return _sync_datetime_fields(df)

    @staticmethod
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

            if elapsed >= _Torn._TORNADO_LONVEVITY_LIMIT:
                # the longest-lived tornado as of 2017 is the tri-state tornado (3.5 hr)
                # anything greater than 4 is certainly suspicious.
                if torlen < 0.3:
                    # short-lived tornado, we assume brief touchdown
                    return begin
                elif end >= begin + _Torn._ONE_DAY:
                    # if possibly not a brief touchdown, assume end-time was entered with wrong day.
                    return elapsed % _Torn._ONE_DAY + begin
                else:
                    # fall back to brief touchdown if no other information
                    return begin
            elif elapsed >= _Torn._ONE_HOUR:
                mph = torlen / (elapsed.seconds / 3600)
                # assume off-by-one error in hour if tornado is moving erroneously slowly
                if mph < 8:
                    hours = elapsed.seconds // 3600
                    return elapsed % _Torn._ONE_HOUR + pd.Timedelta(hours - 1) + begin
            # don't correct
            return end

        if end_date_time >= begin_date_time:
            result_begin, result_end = begin_date_time, _correct_only_end(begin_date_time, end_date_time)
        else:
            elapsed_rev = begin_date_time - end_date_time
            if elapsed_rev < _Torn._TORNADO_LONVEVITY_LIMIT:
                # assume times were accidentally swapped in entries
                new_end_time, new_begin_time = begin_date_time, end_date_time
                result_begin, result_end = new_begin_time, _correct_only_end(new_begin_time, new_end_time)
            else:
                # off-by-one error in date entry
                result_begin, result_end = begin_date_time, end_date_time + _Torn._ONE_DAY

        return [result_begin, result_end]

    @staticmethod
    def points(torn, delta=None):
        if delta is None:
            delta = _Torn._ONE_MINUTE

        elapsed_min = _Torn.longevity(torn) / _Torn._ONE_MINUTE
        spacing_min = delta / _Torn._ONE_MINUTE
        slat, slon, elat, elon = torn.begin_lat, torn.begin_lon, torn.end_lat, torn.end_lon

        numpoints = elapsed_min // spacing_min + 1
        lat_space = np.linspace(slat, elat, numpoints)
        lon_space = np.linspace(slon, elon, numpoints)

        return np.vstack([lat_space, lon_space]).T


tors = _Torn()


### UTILITIES ###


def _sync_datetime_fields(df, tz=None):

    def _extract(prefix):
        dt_col = '{}_date_time'.format(prefix)
        if dt_col in df.columns:
            dts = df[dt_col].dt

            yearmonth_col = '{}_yearmonth'.format(prefix)
            time_col = '{}_time'.format(prefix)
            day_col = '{}_day'.format(prefix)

            if yearmonth_col in df.columns:
                df[yearmonth_col] = dts.strftime('%Y%m')
            if time_col in df.columns:
                df[time_col] = dts.strftime('%H%M')
            if day_col in df.columns:
                df[day_col] = dts.day

            if prefix == 'begin' and 'year' in df.columns:
                df['year'] = dts.year

            if prefix == 'begin' and 'month_name' in df.columns:
                df['month_name'] = dts.strftime('%B')

    _extract('begin')
    _extract('end')

    if tz is not None:
        df['cz_timezone'] = tz

    return df


def export(df, saveloc, **kwargs):
    df.to_csv(saveloc, header=[col.upper() for col in df.columns], index=False, **kwargs)
