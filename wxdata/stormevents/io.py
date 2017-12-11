import re
import warnings
from datetime import datetime
from functools import partial
from itertools import product
import six

import pandas as pd

from wxdata.common import get_links, DataRetrievalException
from wxdata.stormevents.temporal import convert_df_tz
from wxdata.workdir import bulksave
from wxdata import _timezones as _tz

__all__ = ['load_file', 'load_events', 'load_events_year', 'export',
           'tornadoes', 'hail', 'all_severe', 'tstorm_wind', 'urls_for']


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
                            in product(('BEGIN', 'END'), ('YEARMONTH',))},
                     compression='infer')

    df.columns = map(str.lower, df.columns)

    if eventtypes is not None:
        df = df[df.event_type.isin(eventtypes)]
    if states is not None:
        df = df[df.state.isin([state.upper() for state in states])]

    if tz:
        df = convert_df_tz(df, tz, False)
    if keep_data_start and keep_data_end:
        if tz:
            keep_data_start = keep_data_start.tz_localize(_tz.parse_tz(tz))
            keep_data_end = keep_data_end.tz_localize(_tz.parse_tz(tz))

        df = df[(df.begin_date_time >= keep_data_start) & (df.begin_date_time < keep_data_end)]

    return df


def load_events(start, end, eventtypes=None, states=None, tz=None, debug=False):
    if isinstance(start, six.string_types):
        start = pd.Timestamp(start)
    if isinstance(end, six.string_types):
        end = pd.Timestamp(end)

    if tz is not None:
        start = start.tz_localize(_tz.parse_tz(tz))
        end = end.tz_localize(_tz.parse_tz(tz))

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


def export(df, saveloc, **kwargs):
    df.to_csv(saveloc, header=[col.upper() for col in df.columns], index=False, **kwargs)


tornadoes = partial(load_events, eventtypes=['Tornado'])
hail = partial(load_events, eventtypes=['Hail'])
tstorm_wind = partial(load_events, eventtypes=['Thunderstorm Wind'])
all_severe = partial(load_events, eventtypes=['Tornado', 'Hail', 'Thunderstorm Wind'])