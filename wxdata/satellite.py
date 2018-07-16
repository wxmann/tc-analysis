from siphon.catalog import TDSCatalog

import pandas as pd
import xarray as xr

_GRIDSAT_GOES_REGEX = r'(?P<year>\d{4})\.(?P<month>\d{2})\.(?P<day>\d{2})\.(?P<hour>\d{2})(?P<minute>\d{2})'


def gridsat_goes_query(dt, bbox, var, return_type='xarray', catalog=None, sat=None):
    dt = pd.Timestamp(dt)
    if catalog is None:
        cat_url = 'https://www.ncei.noaa.gov/thredds/catalog/' \
                  'satellite/gridsat-goes-full-disk/{dt:%Y}/{dt:%m}/catalog.xml'.format(dt=dt)
        catalog = TDSCatalog(cat_url)

    if sat is not None:
        regex = '{}\.'.format(sat) + _GRIDSAT_GOES_REGEX
    else:
        regex = _GRIDSAT_GOES_REGEX

    return _get_subset(catalog, dt, bbox, var, regex, return_type)


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
