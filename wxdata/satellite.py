from siphon.catalog import TDSCatalog

import pandas as pd
import xarray as xr


def gridsat_goes_query(dt, bbox, var, return_type='xarray', cat=None):
    dt = pd.Timestamp(dt)
    if cat is None:
        cat_url = 'https://www.ncei.noaa.gov/thredds/catalog/' \
                  'satellite/gridsat-goes-full-disk/{dt:%Y}/{dt:%m}/catalog.xml'.format(dt=dt)
        cat = TDSCatalog(cat_url)

    regex = r'(?P<year>\d{4})\.(?P<month>\d{2})\.(?P<day>\d{2})\.(?P<hour>\d{2})(?P<minute>\d{2})'
    return _get_subset(cat, dt, bbox, var, regex, return_type)


def _get_subset(cat, dt, bbox, var, time_regex, return_type='xarray'):
    lon0, lon1, lat0, lat1 = bbox

    found_ds = cat.datasets.filter_time_nearest(dt, regex=time_regex)
    ncss = found_ds.subset()
    query = ncss.query()
    query.lonlat_box(north=lat1, south=lat0, east=lon1, west=lon0)
    query.variables(var)

    data = ncss.get_data(query)
    if return_type == 'netcdf':
        return data
    elif return_type == 'xarray':
        return xr.open_dataset(xr.backends.NetCDF4DataStore(data))
    else:
        raise ValueError("Invalid return, must be `xarray` or `netcdf`")