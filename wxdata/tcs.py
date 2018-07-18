import pandas as pd

from wxdata.workdir import savefile

_LATEST_IBTRACS_VERSION = 'v03r10'


def ibtracs_year(year, basin=None, storm_name=None, storm_num=None, kind='wmo'):
    url = 'ftp://eclipse.ncdc.noaa.gov/pub/ibtracs/{version}/{kind}/csv/year/' \
          'Year.{yr}.ibtracs_wmo.{version}.csv'.format(yr=year, version=_LATEST_IBTRACS_VERSION,
                                                       kind=kind)
    return _load_ibtracs_df(url, basin, storm_name, storm_num)


def _load_ibtracs_df(url, basin, storm_name, storm_num):
    save_to_local = savefile(url, in_subdir='ibtracs')

    df = pd.read_csv(save_to_local.dest, header=1, parse_dates=['ISO_time'])
    df.columns = [col.replace('(WMO)', '').replace(' ', '_').lower() for col in df.columns]

    if basin:
        df = df[df.basin == basin]
    if storm_name:
        df = df[df.name == storm_name]
    if storm_num:
        df = df[df.num == storm_num]
    return df