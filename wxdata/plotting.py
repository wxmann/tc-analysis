import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import matplotlib.patheffects as path_effects
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from mpl_toolkits.basemap import Basemap

from wxdata.config import get_resource


# TODO: add tests for functions in this module!

def plot_points(pts, basemap, color='k', marker='o', markersize=2, **kwargs):
    if isinstance(pts, pd.DataFrame):
        latlons = pts[['lat', 'lon']].as_matrix()
    else:
        latlons = pts

    lons = latlons[:, 1]
    lats = latlons[:, 0]
    x, y = basemap(lons, lats)
    basemap.plot(x, y, marker, markersize=markersize, color=color, **kwargs)


def plot_time_progression(pts, basemap, time_buckets, colormap,
                          legend=None, legend_handle_func=None, marker='o', markersize=2, **kwargs):
    assert isinstance(pts, pd.DataFrame)

    buckets = list(time_buckets)
    colors = sample_colors(len(buckets), colormap)

    for time_bucket_start, time_bucket_end in buckets:
        hour_pts = pts[(pts.timestamp >= time_bucket_start) & (pts.timestamp < time_bucket_end)]
        color = next(colors)
        plot_points(hour_pts, basemap, color, marker=marker, markersize=markersize, **kwargs)

        if legend is not None:
            if legend_handle_func is None:
                leg_label = '{} to {}'.format(time_bucket_start.strftime('%Y-%m-%d %H:%M'),
                                              time_bucket_end.strftime('%Y-%m-%d %H:%M'))
            else:
                leg_label = legend_handle_func(time_bucket_start, time_bucket_end)
            legend.append(color, leg_label)


def plot_cities(city_coordinates, basemap,
                color='k', marker='+', markersize=9, labelsize=12, alpha=0.6, dx=0.05, dy=-0.15):
    for city in city_coordinates:
        coords = city_coordinates[city]
        x, y = basemap(*reversed(coords))
        basemap.plot(x, y, marker, markersize=markersize, color=color, alpha=alpha)

        labelx, labely = basemap(coords[1] + dx, coords[0] + dy)
        plt.text(labelx, labely, city, fontsize=labelsize, color=color, alpha=alpha)


def bottom_right_textbox(ax, text, fontsize=16):
    plt.text(0.99, 0.01, text, transform=ax.transAxes, fontsize=fontsize,
             verticalalignment='bottom', horizontalalignment='right',
             bbox=dict(alpha=0.75, facecolor='white', edgecolor='gray'))


def top_right_textbox(ax, text, fontsize=16):
    plt.text(0.99, 0.99, text, transform=ax.transAxes, fontsize=fontsize,
             verticalalignment='top', horizontalalignment='right',
             bbox=dict(alpha=0.75, facecolor='white', edgecolor='gray'))


def top_left_textbox(ax, text, fontsize=16):
    plt.text(0.01, 0.99, text, transform=ax.transAxes, fontsize=fontsize,
             verticalalignment='top', horizontalalignment='left',
             bbox=dict(alpha=0.75, facecolor='white', edgecolor='gray'))


def simple_basemap(bbox, proj='merc', resolution='i', ax=None,
                   us_detail=True, draw=None):
    llcrnrlon, urcrnrlon, llcrnrlat, urcrnrlat = bbox[:4]

    m = Basemap(projection=proj, ax=ax,
                llcrnrlon=llcrnrlon, llcrnrlat=llcrnrlat, urcrnrlon=urcrnrlon, urcrnrlat=urcrnrlat,
                resolution=resolution, area_thresh=1000)

    if draw is None:
        draw = ['coastlines', 'countries', 'states']
        if us_detail:
            draw += ['counties', 'highways']

    if 'coastlines' in draw:
        m.drawcoastlines()
    if 'countries' in draw:
        m.drawcountries()
    if 'states' in draw:
        m.drawstates()
    if 'counties' in draw:
        m.drawcounties()
    if 'highways' in draw:
        draw_hways(m)

    return m


def draw_hways(basemap, color='red', linewidth=0.33, ax=None):
    basemap.readshapefile(get_resource('hways/hways'), 'hways', drawbounds=True,
                          color=color, linewidth=linewidth, ax=ax)


class LegendBuilder(object):
    def __init__(self, **legend_kw):
        self.handles = []
        self.legend_kw = legend_kw

    def append(self, color, label, **kwargs):
        self.handles.append(mpatches.Patch(color=color, label=label, **kwargs))

    def plot_legend(self):
        plt.legend(handles=self.handles, **self.legend_kw)


def draw_latlon_box(basemap, latlon_bbox, facecolor='none', edgecolor='red', linewidth=3, **kwargs):
    lon1, lon2, lat1, lat2 = latlon_bbox[:]
    x1, y1 = basemap(lon1, lat1)
    x2, y2 = basemap(lon1, lat2)
    x3, y3 = basemap(lon2, lat2)
    x4, y4 = basemap(lon2, lat1)
    box = mpatches.Polygon([(x1, y1), (x2, y2), (x3, y3), (x4, y4)],
                           facecolor=facecolor, edgecolor=edgecolor, linewidth=linewidth, **kwargs)
    basemap.ax.add_patch(box)


def shadow(offset=(0.5, 0.5), alpha=0.6):
    return path_effects.withSimplePatchShadow(offset=offset, alpha=alpha, shadow_rgbFace='black')


def inset_colorbar(mappable, ax, width='60%', height='3%', loc=1, tickcolor='k', ticksize='large',
                   title=None, titlecolor='k', titlesize=14):
    cbar_ax = inset_axes(ax, width=width, height=height, loc=loc)
    cbar = plt.colorbar(mappable, cax=cbar_ax, orientation='horizontal')
    cbar.ax.xaxis.set_tick_params(which='major', direction='in')

    text_shadow = shadow()
    plt.setp(plt.getp(cbar.ax.axes, 'xticklabels'), color=tickcolor, size=ticksize, y=1.0,
             path_effects=[text_shadow])
    if title:
        cbar.set_label(title, color=titlecolor, path_effects=[text_shadow], fontsize=titlesize)


## Color sampling


def sample_colors(n, src_cmap):
    return ColorSamples(n, src_cmap)


# inspired from http://qingkaikong.blogspot.com/2016/08/clustering-with-dbscan.html
class ColorSamples(object):
    def __init__(self, n_samples, cmap):
        self.n_samples = n_samples
        color_norm = mcolors.Normalize(vmin=0, vmax=self.n_samples - 1)
        self.scalar_map = cm.ScalarMappable(norm=color_norm, cmap=cmap)

        self._iter_index = 0

    def __getitem__(self, item):
        return self.scalar_map.to_rgba(item)

    def __iter__(self):
        return self

    def __next__(self):
        if self._iter_index >= self.n_samples:
            raise StopIteration
        ret = self.scalar_map.to_rgba(self._iter_index)
        self._iter_index += 1
        return ret

    next = __next__

    def __len__(self):
        return self.n_samples
