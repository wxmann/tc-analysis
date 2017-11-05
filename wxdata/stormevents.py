import re
from datetime import datetime
from functools import partial
from itertools import product

import pandas as pd
import pytz
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
                     # infer_datetime_format=True,
                     compression='infer')
    df.columns = map(str.lower, df.columns)

    if tz:
        df = convert_df_tz(df, tz, False)
    if keep_data_start and keep_data_end:
        df = df[(df.begin_date_time >= keep_data_start) & (df.begin_date_time < keep_data_end)]
    if eventtypes:
        df = df[df.event_type.isin(eventtypes)]
    if states:
        df = df[df.state.isin([state.upper() for state in states])]

    return df


def load_events(start, end, eventtypes=None, states=None, tz=None):
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
        import warnings
        err_yrs = [str(_year_from_link(err.url)) for err in errors]
        warnings.warn('There were errors trying to load dataframes for years: {}'.format(','.join(err_yrs)))

    if dfs:
        ret = pd.concat(dfs, ignore_index=True)
        return ret[(ret.begin_date_time >= start) & (ret.begin_date_time < end)]
    else:
        return pd.DataFrame()


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
    if copy:
        df = df.copy()

    def _cols_in_df(*cols):
        return (c for c in cols if c in df.columns)

    for col in _cols_in_df('begin_date_time', 'end_date_time'):
        df[col] = df.apply(lambda row: convert_row_tz(row, col, to_tz), axis=1)

    for flag, temporal_accessor in product(('begin', 'end'), ('yearmonth', 'time', 'day')):
        col = '{}_{}'.format(flag, temporal_accessor)
        src = '{}_date_time'.format(flag)

        if col in df.columns and src in df.columns:
            def get_val(row):
                if temporal_accessor == 'yearmonth':
                    return row[src].strftime('%Y%m')
                elif temporal_accessor == 'time':
                    return row[src].strftime('%H%M')
                elif temporal_accessor == 'day':
                    return row[src].day
                else:
                    raise ValueError("This should not happen -- location: convert DF timezone")
            df[col] = df.apply(get_val, axis=1)

    for col in _cols_in_df('month_name'):
        if 'begin_date_time' in df.columns:
            df[col] = df.apply(lambda row: row['begin_date_time'].strftime('%B'), axis=1)

    df['cz_timezone'] = to_tz
    return df


def convert_row_tz(row, col, to_tz):
    try:
        return convert_timestamp_tz(row[col], row['cz_timezone'], to_tz)
    except ValueError as e:
        import warnings
        warnings.warn("Encountered an error while converting time zone, {}. "
                      "\nAttempting to use location data to determine tz".format(repr(e)))
        try:
            state = row['state']
            if row.cz_timezone == 'AST':
                if state == 'ALASKA':
                    return convert_timestamp_tz(row[col], 'AKST-9', to_tz)
                else:
                    # both Puerto Rico and Virgin Islands in AST
                    return convert_timestamp_tz(row[col], 'AST-4', to_tz)
            else:
                tz = _tzhelp.tz_for_state(state)
                if tz:
                    return tz.localize(row[col]).tz_convert(to_tz)
                else:
                    lat, lon = row['begin_lat'], row['begin_lon']
                    tz = _tzhelp.tz_for_latlon(lat, lon)
                    dummy_time = pd.Timestamp('2017-01-01')
                    offset_time = dummy_time + tz.utcoffset(dummy_time, is_dst=False)

                    if offset_time < dummy_time:
                        hour_offset = 24 - offset_time.hour
                        return convert_timestamp_tz(row[col], 'Etc/GMT+{}'.format(hour_offset), to_tz)
                    else:
                        return convert_timestamp_tz(row[col], 'Etc/GMT-{}'.format(offset_time.hour), to_tz)
        except ImportError:
            warnings.warn("Can't find timezone from location without the `geopy` module."
                          "Please install that module.")
        except KeyError:
            warnings.warn("Can't find timezone with missing lat lon or state data.")

        raise e
    except KeyError:
        raise ValueError("Row must have column specific and `cz_timezone` column")


def convert_timestamp_tz(timestamp, from_tz, to_tz):
    original_tz_pd = _pdtz_from_str(from_tz)
    new_tz_pd = _pdtz_from_str(to_tz)
    return pd.Timestamp(timestamp, tz=original_tz_pd).tz_convert(new_tz_pd)


def _pdtz_from_str(tz_str):
    if not tz_str or tz_str in ('UTC', 'GMT'):
        return tz_str

    tz_str_up = tz_str.upper()
    # handle the weird cases
    if tz_str_up in ('SCT', 'CSC'):
        # found these egregious typos
        raise ValueError("{} is probably CST but cannot determine for sure".format(tz_str))
    elif tz_str_up == 'UNK':
        raise ValueError("UNK timezone")
    elif tz_str_up == 'AST':
        # In older versions of storm events, `AST` and `AKST` are both logged as `AST`.
        # We can't let our tz-conversion logic believe naively it's Atlantic Standard Time
        raise ValueError("Ambiguous timezone `AST`; either Alaska or Atlantic standard time")
    elif tz_str_up == 'GST10':
        # this is how Guam entries are logged
        return pytz.timezone('Etc/GMT+10')

    # we're safe; fallback to our usual timezone parsing logic
    return _tzhelp.parse_tz(tz_str)
