import pandas as pd
import six
import xarray as xr
from datetime import timedelta
import multiprocessing as mp

from wxdata import uaplots
from wxdata.http import tds_dataset_url

CFSR_PARENT = 'https://www.ncei.noaa.gov/thredds/catalog'

CFRS_DAP = CFSR_PARENT.replace('catalog', 'dodsC')

GRB2_FORMAT = 'grb2'

GRIB2_FORMAT = 'grib2'

CFS_REANALYSIS = 'cfs_reanl'

CFS_V2 = 'cfs_v2_anl'


def cfs_6h_dataset(var, time, fcst=None, debug=False, opener=xr.open_dataset):
    datetime_ = _maybe_convert_str(time)
    kw = dict(fcst=fcst, debug=debug, opener=opener)
    if datetime_ >= pd.Timestamp('2011-04-01 00:00'):
        ds = cfsv2_6h(var, time, **kw)
    else:
        ds = cfsr_6h(var, time, **kw)
    return ds


def cfsr_6h(var, time, fcst=None, debug=False, opener=xr.open_dataset):
    time = _maybe_convert_str(time)
    catalog_url = '{par}/{cat}_6h_{var}/{ts:%Y}/{ts:%Y%m}/{ts:%Y%m%d}/'.format(cat=CFS_REANALYSIS, par=CFRS_DAP,
                                                                               var=var, ts=time)
    # example: pgbhnl.gdas.1999050300.grb2
    ds_name = '{var}h{fcst}.gdas.{ts:%Y%m%d%H}.{frmt}'.format(var=var,
                                                              fcst='nl' if fcst is None else str(fcst).zfill(2),
                                                              ts=time,
                                                              frmt=GRB2_FORMAT)
    return _open_cfs_dataset(catalog_url, ds_name, debug, opener, _tds=False)


def cfsv2_6h(var, time, fcst=None, debug=False, opener=xr.open_dataset):
    time = _maybe_convert_str(time)

    if var == 'pgb':
        var2 = 'p'
    else:
        var2 = var

    catalog_url = '{par}/{cat}_6h_{var}/{ts:%Y}/{ts:%Y%m}/{ts:%Y%m%d}/'.format(cat=CFS_V2, par=CFRS_DAP,
                                                                               var=var, ts=time)
    # example: cdas1.t12z.pgrbhanl.grib2
    ds_name = 'cdas1.t{ts:%Hz}.{var}grbh{fcst}.{frmt}'.format(ts=time, var=var2,
                                                              fcst='anl' if fcst is None else str(fcst).zfill(2),
                                                              frmt=GRIB2_FORMAT)
    return _open_cfs_dataset(catalog_url, ds_name, debug, opener, _tds=False)


def cfs_6h_mfdataset(var, times, fcst=None, debug=False, yield_single_times=False):
    urls = [cfs_6h_dataset(var, time, fcst=fcst, debug=debug, opener=None) for time in times]
    mf = xr.open_mfdataset(urls, concat_dim='time')
    if yield_single_times:
        return ((time, mf.sel(time=time)) for time in times)
    else:
        return mf


def _open_cfs_dataset(catalog_url, ds_name, debug, opener, _tds=False):
    if _tds:
        ds_url = tds_dataset_url(catalog_url.replace('dodsC', 'catalog') + 'catalog.xml', ds_name)
    else:
        ds_url = catalog_url + ds_name
    if debug:
        print('Obtained dataset: {}'.format(ds_url))

    if opener is None:
        return ds_url
    return opener(ds_url)


def _maybe_convert_str(time, conversion=None):
    if conversion is None:
        conversion = lambda ts: pd.Timestamp(ts)

    if isinstance(time, six.string_types):
        return conversion(time)

    return time

# TODO: deprecate the below two functions which don't leverage the parallelization built into dask and are slow


def cfsr_6h_range(var, start_time, end_time, timestep='6 hr', **open_kw):
    start_time = _maybe_convert_str(start_time)
    end_time = _maybe_convert_str(end_time)
    timestep = _maybe_convert_str(timestep, conversion=lambda ts: pd.Timedelta(ts).to_pytimedelta())

    limit_days = 60
    if end_time - start_time > timedelta(days=limit_days):
        raise ValueError("Please limit your CFSR query to less than or equal to {} days".format(limit_days))

    analysis_time = start_time
    while analysis_time < end_time:
        yield cfsr_6h(var, analysis_time, **open_kw)
        analysis_time += timestep


def cfsr_6h_apply(var, start_time, end_time, timestep='6 hr', apply=None, parallelize=5):
    urls = list(cfsr_6h_range(var, start_time, end_time, timestep, opener=None))

    if apply is None:
        apply = xr.open_dataset

    if parallelize <= 1:
        return list(map(apply, urls))

    with mp.Pool(parallelize) as pool:
        return pool.map(apply, urls)


### PLOTTING


# TODO: deprecate, we can just use `plot_h5_anomaly_dataset` function
def plot_h5_anomaly(time, basemap, debug=True, subset=True, **plot_kw):
    try:
        times = iter(time)
        ds = cfs_6h_mfdataset('pgb', times, debug=debug)
    except TypeError:
        ds = cfs_6h_dataset('pgb', time, debug=debug)

    return plot_h5_anomaly_dataset(ds, basemap, subset, **plot_kw)


def plot_h5_anomaly_dataset(xr_dataset, basemap, subset=True, close_dataset=True, **plot_kw):
    try:
        # TODO: figure out subsetting longitude data and 180 -> 360 longitude conversion
        if subset:
            lat_query = {'lat': slice(basemap.latmax, basemap.latmin)}
        else:
            lat_query = {}

        anom = xr_dataset['Geopotential_height_anomaly_isobaric'].sel(isobaric2=50000, **lat_query)
        hgt = xr_dataset['Geopotential_height_isobaric'].sel(isobaric3=50000, **lat_query)

        lons = hgt.lon.values
        lats = hgt.lat.values

        if 'time' in anom.dims:
            anom = anom.sum('time').values
        else:
            anom = anom.values

        if 'time' in hgt.dims:
            hgt = hgt.sum('time').values
        else:
            hgt = hgt.values

        return uaplots.h5_anom_plot(lats, lons, hgt, anom, basemap, **plot_kw)
    finally:
        if close_dataset:
            xr_dataset.close()
