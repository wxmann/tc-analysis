import numpy as np


def lat_weighted_spread(raw_spread, spread_var, normalize_by=1.0, lat_dim='lat', band=2.5,
                        reducer=np.median):
    lat_indexed_values = {}
    for lat in raw_spread[lat_dim].values:
        lat_slice = raw_spread.sel(**{lat_dim: slice(lat - band, lat + band)})
        lat_indexed_values[lat] = lat_slice.reduce(reducer).item()

    temp_df = raw_spread.to_dataframe().reset_index()
    temp_df[spread_var] = temp_df.apply(lambda r: (r[spread_var] - lat_indexed_values[r[lat_dim]]) / normalize_by,
                                        axis=1)
    result_df = temp_df.set_index(['lat', 'lon'])[[spread_var]]

    return result_df.to_xarray()[spread_var]
