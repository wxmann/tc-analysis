import pandas as pd
import six
import xarray as xr
from datetime import timedelta
import multiprocessing as mp

from wxdata.http import tds_dataset_url

CFSR_PARENT = 'https://www.ncei.noaa.gov/thredds/catalog'

CFRS_DAP = CFSR_PARENT.replace('catalog', 'dodsC')

CFSR_FORMAT = 'grb2'


def cfsr_6h(var, time, fcst=None, debug=False, opener=xr.open_dataset, _tds=False):
    time = _maybe_convert_str(time)
    catalog_url = '{par}/cfs_reanl_6h_{var}/{ts:%Y}/{ts:%Y%m}/{ts:%Y%m%d}/'.format(par=CFRS_DAP,
                                                                                   var=var, ts=time)
    # example: pgbhnl.gdas.1999050300.grb2
    ds_name = '{var}h{fcst}.gdas.{ts:%Y%m%d%H}.{frmt}'.format(var=var,
                                                              fcst='nl' if fcst is None else str(fcst).zfill(2),
                                                              ts=time,
                                                              frmt=CFSR_FORMAT)
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
