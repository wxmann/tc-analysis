import pandas as pd
import xarray as xr


def gfs_ensembles(time_, var=None, yield_single_times=False):
    try:
        time_ = pd.Timestamp(time_)
    except TypeError:
        time_ = [pd.Timestamp(ts) for ts in time_]

    if not isinstance(time_, list):
        url = 'http://nomads.ncep.noaa.gov:9090/dods/gens/gens{0:%Y%m%d}/gep_all_{0:%H}z'.format(time_)
        ds = xr.open_dataset(url)
    else:
        urls = ['http://nomads.ncep.noaa.gov:9090/dods/gens/gens{0:%Y%m%d}/gep_all_{0:%H}z'.format(ts)
                for ts in time_]
        if yield_single_times:
            return _yield_gfse_datasets(urls, var)

        ds = xr.open_mfdataset(urls, chunks={'lat': 10, 'lon': 10})

    return ds if var is None else ds[var]


def _yield_gfse_datasets(urls, var):
    for url in urls:
        ds = xr.open_dataset(url)
        yield ds if var is None else ds[var]