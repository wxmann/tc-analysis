import cartopy.crs as ccrs
import cartopy.feature as cfeat
import matplotlib.pyplot as plt


def get_map(proj=None, extent=None):
    if proj is None:
        proj = ccrs.PlateCarree()
    ax = plt.axes(projection=proj)

    if extent and len(extent) == 4:
        ax.set_extent(extent)

    ax.coastlines()
    states = cfeat.NaturalEarthFeature(category='cultural', name='admin_1_states_provinces_lakes',
                                       scale='50m', facecolor='none')
    ax.add_feature(states, edgecolor='black', linewidth=1.0)
    return ax


def plot_point(ax, lat, lon, *args, **kwargs):
    ax.plot(lat, lon, *args, transform=ccrs.Geodetic(), **kwargs)
