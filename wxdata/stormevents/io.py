import re
import warnings
from datetime import datetime
from functools import partial
from itertools import product

import pandas as pd
import six

from wxdata.http import get_links, DataRetrievalException
from wxdata.stormevents.temporal import convert_df_tz, localize_timestamp_tz, convert_timestamp_tz
from wxdata.workdir import bulksave

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


def load_file(file, keep_data_start=None, keep_data_end=None, months=None, hours=None,
              eventtypes=None, states=None, tz=None, tz_localize=False):
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

    if tz_localize:
        # hack to restore tz information after loading the file
        # most times this step will not be needed. This is just for testing and dataframe comparison
        # TODO: can we implement a localize dataframe function in the temporal processing library?

        # We can't use the localize_timestamp_tz function here because pandas
        # converts timestamps to UTC time, then makes them tz-naive. The localize function
        # assumes the timestamps are naive but in the correct timezone.
        df['begin_date_time'] = df.apply(
            lambda r: convert_timestamp_tz(r.begin_date_time, 'UTC', r.cz_timezone), axis=1)
        df['end_date_time'] = df.apply(
            lambda r: convert_timestamp_tz(r.end_date_time, 'UTC', r.cz_timezone), axis=1)

    if eventtypes is not None:
        df = df[df.event_type.isin(eventtypes)]
    if states is not None:
        df = df[df.state.isin([state.upper() for state in states])]
    if months is not None:
        df = df[df.month_name.isin(months)]
    if hours is not None:
        df = df[pd.to_numeric(df.begin_time.str[:2]).isin(hours)]

    if keep_data_start and keep_data_end:
        if tz:
            keep_data_start = localize_timestamp_tz(keep_data_start, tz)
            keep_data_end = localize_timestamp_tz(keep_data_end, tz)

            # if we're looking at small date range, we don't have to convert the TZ for the
            # entire DF, which is expensive. But we do have to account not shifting TZ can cause
            # a +/- 1 day error.
            start_m1days = keep_data_start - pd.Timedelta(days=1)
            end_p1days = keep_data_end + pd.Timedelta(days=1)

            df = df[(df.begin_date_time >= start_m1days) & (df.begin_date_time < end_p1days)]
            df = convert_df_tz(df, tz, False)

        df = df[(df.begin_date_time >= keep_data_start) & (df.begin_date_time < keep_data_end)]
    elif tz:
        df = convert_df_tz(df, tz, False)

    return df


def load_events(start, end, eventtypes=None, states=None, months=None,
                hours=None, tz=None, debug=False):

    if isinstance(start, six.string_types):
        start = pd.Timestamp(start)
    if isinstance(end, six.string_types):
        end = pd.Timestamp(end)

    if tz is not None:
        start = localize_timestamp_tz(start, tz)
        end = localize_timestamp_tz(end, tz)

    #FIXME: there is a corner case of start and end being near the turn of the year that fails.
    # We need to resolve the start and end variables to able to load both years'
    # dataframes if needed.

    if end < start:
        raise ValueError("End date must be on or after start date")
    year1 = start.year
    year2 = end.year

    if year1 == year2:
        links = urls_for([year1])
        load_df_with_filter = partial(load_file, keep_data_start=start, keep_data_end=end,
                                      eventtypes=eventtypes, states=states, months=months,
                                      hours=hours, tz=tz)
    else:
        links = urls_for(range(year1, year2 + 1))
        load_df_with_filter = partial(load_file, keep_data_start=None, keep_data_end=None,
                                      eventtypes=eventtypes, states=states, months=months,
                                      hours=hours, tz=tz)

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


def export(df, saveloc, lowercase_cols=True, **kwargs):
    if lowercase_cols:
        df.to_csv(saveloc, header=[col.upper() for col in df.columns], index=False, **kwargs)
    else:
        df.to_csv(saveloc, index=False, **kwargs)


tornadoes = partial(load_events, eventtypes=['Tornado'])
hail = partial(load_events, eventtypes=['Hail'])
tstorm_wind = partial(load_events, eventtypes=['Thunderstorm Wind'])
all_severe = partial(load_events, eventtypes=['Tornado', 'Hail', 'Thunderstorm Wind'])