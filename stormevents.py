import re
from datetime import datetime

import pandas as pd

from common import get_links, DataRetrievalException
from workdir import bulksave


def urls_for(years):
    links = get_links('https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/')
    for link in links:
        try:
            year = _year_from_link(link)
            if int(year) in years:
                yield link
        except DataRetrievalException:
            pass


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
              eventtypes=None, states=None):
    df = pd.read_csv(file,
                     parse_dates=['BEGIN_DATE_TIME', 'END_DATE_TIME'],
                     compression='infer')
    df.columns = map(str.lower, df.columns)

    if keep_data_start and keep_data_end:
        df = df[(df.begin_date_time >= keep_data_start) & (df.begin_date_time < keep_data_end)]
    if eventtypes:
        eventtypes1 = [e[:1].upper() + e[1:].lower() for e in eventtypes]
        df = df[df.event_type.isin(eventtypes1)]
    if states is not None:
        df = df[df.state.isin([state.upper() for state in states])]

    return df


def load_events(start, end, eventtypes=None, states=None):
    if end < start:
        raise ValueError("End date must be on or after start date")
    year1 = start.year
    year2 = end.year
    links = urls_for(range(year1, year2 + 1))

    dfs = []

    def load_df_with_filter(file):
        df = load_file(file, start, end, eventtypes, states)
        dfs.append(df)

    successes, errors = bulksave(links, postsave=load_df_with_filter)
    if errors:
        import warnings
        err_yrs = [str(_year_from_link(err_link)) for err_link in errors]
        warnings.warn('There were errors trying to load dataframes for years: {}'.format(','.join(err_yrs)))

    if dfs:
        return pd.concat(dfs)
    else:
        return pd.DataFrame()


def load_events_year(year, eventtypes=None, states=None):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31, 23, 59)
    return load_events(start, end, eventtypes, states)
