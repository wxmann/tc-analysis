import re
from datetime import datetime
from functools import partial
from itertools import product

import pandas as pd
import pytz
import six

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
                     compression='infer')
    df.columns = map(str.lower, df.columns)

    if keep_data_start and keep_data_end:
        df = df[(df.begin_date_time >= keep_data_start) & (df.begin_date_time < keep_data_end)]
    if eventtypes:
        df = df[df.event_type.isin(eventtypes)]
    if states:
        df = df[df.state.isin([state.upper() for state in states])]
    if tz:
        df = standardize_df_tz(df, tz, False)

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
        ret = pd.concat(dfs)
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


def standardize_df_tz(df, tz='CST', copy=True):
    if copy:
        df = df.copy()

    def _cols_in_df(*cols):
        return (c for c in cols if c in df.columns)

    for col in _cols_in_df('begin_date_time', 'end_date_time'):
        df[col] = df.apply(lambda row: convert_tz(row[col], row.cz_timezone, tz), axis=1)

    for flag, temporal_accessor in product(('begin', 'end'), ('yearmonth', 'time', 'day')):
        col = '{}_{}'.format(flag, temporal_accessor)
        src = '{}_date_time'.format(flag)

        if col in df.columns and src in df.columns:
            # get_val = partial(_get_val, temporal_accessor=temporal_accessor, src=src)
            def get_val(row):
                if temporal_accessor == 'yearmonth':
                    return row[src].strftime('%Y%m')
                elif temporal_accessor == 'time':
                    return row[src].strftime('%H%M')
                elif temporal_accessor == 'day':
                    return row[src].day
                else:
                    raise ValueError("This should not happen -- location: standardize DF timestamps")
            df[col] = df.apply(get_val, axis=1)

    for col in _cols_in_df('month_name'):
        if 'begin_date_time' in df.columns:
            df[col] = df.apply(lambda row: row['begin_date_time'].strftime('%B'), axis=1)

    return df


def convert_tz(timestamp, original_tz, new_tz):
    original_tz_pd = _pdtz_from_str(original_tz)
    new_tz_pd = _pdtz_from_str(new_tz)
    return pd.Timestamp(timestamp, tz=original_tz_pd).tz_convert(new_tz_pd)


def _pdtz_from_str(tz_str):
    if not tz_str:
        return 'UTC'
    try:
        # we're good here; pandas uses pytz to parse timezone strings
        return pytz.timezone(tz_str)
    except pytz.UnknownTimeZoneError:
        # well okay, we gotta do the conversion
        tz_str = tz_str.strip()[:3].upper()

        if tz_str in ('UTC', 'GMT'):
            return tz_str
        elif 'AKST' in tz_str:
            # Hardcode for AKST in newer entries for AK in the Storm Events Database
            return 'Etc/GMT+9'
        elif tz_str[1:] in ('ST', 'DT'):
            first = tz_str[0]
            dst = tz_str[1] == 'D'

            init_delta_map = {
                'H': -10, # HST
                'A': -9,  # AKST (referred to as `AST` in older entries in the Storm Events database)
                'P': -8,  # PST
                'M': -7,  # MST
                'C': -6,  # CST
                'E': -5   # EST
            }

            try:
                delta = init_delta_map[first]
            except KeyError:
                raise ValueError("Invalid tz: {}".format(tz_str))
            delta += dst
            return 'Etc/GMT+{}'.format(-delta)
        else:
            raise ValueError("Invalid tz: {}".format(tz_str))
