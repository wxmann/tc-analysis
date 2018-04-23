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


def cfsr_6h(var, time, fcst=None, debug=False, opener=xr.open_dataset, _tds=False):
    time = _maybe_convert_str(time)
    catalog_url = '{par}/{cat}_6h_{var}/{ts:%Y}/{ts:%Y%m}/{ts:%Y%m%d}/'.format(cat=CFS_REANALYSIS, par=CFRS_DAP,
                                                                               var=var, ts=time)
    # example: pgbhnl.gdas.1999050300.grb2
    ds_name = '{var}h{fcst}.gdas.{ts:%Y%m%d%H}.{frmt}'.format(var=var,
                                                              fcst='nl' if fcst is None else str(fcst).zfill(2),
                                                              ts=time,
                                                              frmt=GRB2_FORMAT)
    return _open_cfs_dataset(catalog_url, ds_name, debug, opener, _tds)


def cfsv2_6h(var, time, fcst=None, debug=False, opener=xr.open_dataset, _tds=False):
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
    return _open_cfs_dataset(catalog_url, ds_name, debug, opener, _tds)


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


def _maybe_convert_str(time, conversion=None):
    if conversion is None:
        conversion = lambda ts: pd.Timestamp(ts).to_pydatetime()

    if isinstance(time, six.string_types):
        return conversion(time)

    return time


### PLOTTING


def plot_h5_anomaly(datetime_, basemap, debug=True, **plot_kw):
    datetime_ = _maybe_convert_str(datetime_)
    if datetime_.year >= 2011:
        ds = cfsv2_6h('pgb', datetime_, debug=debug)
    else:
        ds = cfsr_6h('pgb', datetime_, debug=debug)

    try:
        anom = ds['Geopotential_height_anomaly_isobaric'].sel(isobaric2=50000)
        hgt = ds['Geopotential_height_isobaric'].sel(isobaric3=50000)

        lons = hgt.lon.values
        lats = hgt.lat.values

        anom = anom.sum('time').values
        hgt = hgt.sum('time').values

        return uaplots.h5_anom_plot(lats, lons, hgt, anom, basemap, **plot_kw)
    finally:
        ds.close()
