import re

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


def load_df_from(file):
    raw_data = pd.read_csv(file,
                           parse_dates=['BEGIN_DATE_TIME', 'END_DATE_TIME'],
                           compression='infer')
    raw_data.columns = map(str.lower, raw_data.columns)
    return raw_data


def load(start, end, eventtype):
    if end < start:
        raise ValueError("End date must be on or after start date")
    year1 = start.year
    year2 = end.year
    links = urls_for(range(year1, year2 + 1))

    dfs = []

    def load_df_with_filter(file):
        df = load_df_from(file)
        df = df[(df.begin_date_time >= start) & (df.begin_date_time < end)]
        df = df[df.event_type == eventtype]
        dfs.append(df)

    successes, errors = bulksave(links, postsave=load_df_with_filter)
    if errors:
        import warnings
        warnings.warn('There were errors trying to load a few dataframes')

    return pd.concat(dfs)