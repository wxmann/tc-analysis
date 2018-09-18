import numpy as np
import pandas as pd

from wxdata.geog import angle_between, dist_between
from wxdata.workdir import savefile

_LATEST_IBTRACS_VERSION = 'v03r10'

_BASE_IBTRACS_URL = 'ftp://eclipse.ncdc.noaa.gov/pub/ibtracs'


def ibtracs_year(year, kind='wmo', version=_LATEST_IBTRACS_VERSION):
    path = '/{version}/{kind}/csv/year/Year.{yr}.ibtracs_wmo.{version}.csv'.format(
        yr=year, version=version, kind=kind.lower())

    return _load_ibtracs_df(_BASE_IBTRACS_URL + path)


def ibtracs_basin(basin, kind='wmo', version=_LATEST_IBTRACS_VERSION):
    path = '/{version}/{kind}/csv/basin/Basin.{basin}.ibtracs_{kind}.{version}.csv'.format(
        basin=basin.upper(), version=version, kind=kind.lower())

    return _load_ibtracs_df(_BASE_IBTRACS_URL + path)


def _load_ibtracs_df(url):
    save_to_local = savefile(url, in_subdir='ibtracs')

    df = pd.read_csv(save_to_local.dest, header=1, parse_dates=['ISO_time'], skiprows=[2])
    df.columns = [col.replace('(WMO)', '').replace(' ', '_').lower() for col in df.columns]

    df = df.applymap(lambda x: x.strip() if type(x) is str else x)
    return df


### calcs


def delta(df, quantity, dt='6 hr'):
    series = {}
    dt = pd.Timedelta(dt)
    for _, storm in df.groupby('serial_num'):
        for index, row in storm.iterrows():
            current_time = row.iso_time
            next_time = current_time + dt
            try:
                next_row = storm[storm.iso_time == next_time].iloc[0]
                dquantity = next_row[quantity] - row[quantity]
                series[index] = dquantity
            except (IndexError, KeyError):
                continue

    return pd.Series(series)


def label_ri(df, keep_dwind=False, dwind_col=None):
    labels = {}
    if dwind_col is None:
        df['dwind'] = delta(df, 'wind', dt='24 hours')
    else:
        df['dwind'] = df[dwind_col]

    for _, storm in df.groupby('serial_num'):
        for index, row in storm.iterrows():
            if row['dwind'] >= 30.0:
                init_time = row.iso_time
                labels[index] = True
                internal_index = index

                while True:
                    internal_index += 1
                    try:
                        next_row = storm.loc[internal_index]
                    except KeyError:
                        break
                    next_time = next_row.iso_time

                    if next_time > init_time + pd.Timedelta('24 hours'):
                        labels[next_row.name] = False
                        break
                    else:
                        labels[next_row.name] = True
            elif index not in labels:
                labels[index] = False

    if not keep_dwind or (dwind_col is not None and dwind_col != 'dwind'):
        del df['dwind']
    return pd.Series(labels)


def heading(df, dt='6 hr', to_xy=True, xy_unit='kt'):
    points = []
    dt = pd.Timedelta(dt)
    for _, storm in df.groupby('serial_num'):
        for index, row in storm.iterrows():
            current_time = row.iso_time
            result = {}
            this_row = row
            try:
                next_time = current_time + dt
                next_row = storm[storm.iso_time == next_time].iloc[0]
            except IndexError:
                # we don't have a position at that hour. Maybe we've reached the end?
                continue

            this_point = (this_row.latitude, this_row.longitude)
            next_point = (next_row.latitude, next_row.longitude)

            result['angle'] = angle_between(this_point, next_point)
            result['dist'] = dist_between(this_point, next_point)
            result['name'] = row['name']
            result['season'] = row['season']
            result['iso_time'] = row.iso_time
            result['serial_num'] = row.serial_num

            points.append(result)

    ret = pd.DataFrame(points)

    if to_xy:
        ret['x'] = ret['dist'] * np.cos(ret['angle'])
        ret['y'] = ret['dist'] * np.sin(ret['angle'])

        if xy_unit == 'kt':
            ret['x'] = ret.x * 0.54 / (dt.total_seconds() / 3600)
            ret['y'] = ret.y * 0.54 / (dt.total_seconds() / 3600)

    return ret