import numpy as np
from matplotlib import pyplot as plt, dates as dates

from wxdata.plotting import simple_basemap

__all__ = ['hovmoller_with_map']


def hovmoller_with_map(xrdata, map_bbox, figsize=(12, 16), plot_map_ratio=(6, 1),
                       ylabelsize='x-large', xlabelsize='medium', dayinterval=2, xtickinterval=60,
                       grid=True, datefrmt='%b %-d', grid_kw=None, plotfunc=None, plot_kw=None):
    # setup the subplot layout
    fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=figsize,
                                   gridspec_kw=dict(height_ratios=list(plot_map_ratio)))
    plt.tight_layout()

    # setup the plotting from the data
    if not plotfunc:
        plotfunc = xrdata.plot.contourf
    if not plot_kw:
        plot_kw = dict(center=0, levels=20, yincrease=False)

    for fixed_kw in ('ax', 'add_colorbar', 'add_labels'):
        if fixed_kw in plot_kw:
            plot_kw.pop(fixed_kw)

    mappable = plotfunc(ax=ax1, add_colorbar=False, add_labels=False, **plot_kw)

    # map
    basemap = simple_basemap(map_bbox, proj='cyl', resolution='l', ax=ax2, us_detail=False)

    # gridlines and ticks

    if grid:
        if grid_kw is None:
            grid_kw = dict(color='k', linestyle='-.', linewidth=1, alpha=0.25)
        ax1.grid(**grid_kw)
        ax2.grid(**grid_kw)

    ax1.tick_params(labelsize=ylabelsize)
    if xlabelsize:
        ax2.tick_params(labelsize=xlabelsize)

    lon0, lon1 = map_bbox[:2]
    ax1.xaxis.set_ticks(np.arange(lon0, lon1, xtickinterval))
    ax1.yaxis.set_major_locator(dates.DayLocator(interval=dayinterval))
    ax1.yaxis.set_major_formatter(dates.DateFormatter(datefrmt))

    axes = (ax1, ax2)
    return fig, axes, mappable, basemap