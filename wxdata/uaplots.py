import numpy as np
from mpl_toolkits.basemap import addcyclic
import matplotlib.pyplot as plt

from wxdata.plotting import inset_colorbar


def h5_anom_plot(lats, lons, hgts, anoms, basemap,
                 hgt_levels=None, anom_levels=None, hgt_labels=True, anom_cbar=True):
    if hgt_levels is None:
        hgt_levels = np.arange(4680, 6040, 40)

    if anom_levels is None:
        anom_levels = np.arange(-360, 360, 40)

    hgts, anoms, lons = addcyclic(hgts, anoms, lons)
    xgrid, ygrid = np.meshgrid(lons, lats)
    xgrid, ygrid = basemap(xgrid, ygrid)

    hgt_CS = basemap.contour(xgrid, ygrid, data=hgts,
                             latlon=False, colors='k', levels=hgt_levels, linewidths=0.8)
    if hgt_labels:
        plt.clabel(hgt_CS, hgt_CS.levels[::2], inline=1,
                   inline_spacing=2, fontsize='small', fmt='%d')

    anom_CS = basemap.contourf(xgrid, ygrid, data=anoms,
                               latlon=False, cmap='RdBu_r', levels=anom_levels, extend='both')
    if anom_cbar:
        inset_colorbar(anom_CS, orientation='horizontal', width='50%', title='Anomaly (gpm)')

    return hgt_CS, anom_CS
