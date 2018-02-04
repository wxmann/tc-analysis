import os
import re
from functools import lru_cache

from boto.s3.connection import S3Connection
import numpy as np
import pyart

from datetime import datetime, timedelta

from wxdata import workdir
from wxdata.plotting import draw_hways
from wxdata.utils import log_if_debug

__all__ = ['Level2Archive', 'OrderSelection', 'timestamp_from_key',
           'plot_reflectivity', 'plot_velocity', 'plot_default_display']


class Level2Archive(object):
    def __init__(self):
        self._conn = S3Connection(anon=True)
        self._bucket = self._conn.get_bucket('noaa-nexrad-level2')

    def select_around(self, station, timestamp, dt=timedelta(minutes=3), debug=False):
        timestamp = np.datetime64(timestamp)

        if isinstance(dt, int):
            dt = np.timedelta64(dt, 'm')
        else:
            dt = np.timedelta64(dt)

        min_time = timestamp - dt
        max_time = timestamp + dt
        log_if_debug('Time range considered between {} and {}'.format(min_time, max_time), debug)

        return self.select_between(station, min_time, max_time, debug)

    def select_between(self, station, t1, t2, debug=False, _cache=True):
        t1 = np.datetime64(t1)
        t2 = np.datetime64(t2)

        if t1 >= t2:
            raise ValueError('t1 must be less than t2')
        if t2 - t1 > np.timedelta64(24, 'h'):
            raise ValueError('Queries for time ranges longer than 24h are disallowed.')

        # The Level 2 archive in AWS updates real-time. We want to bypass cached results for
        # queries close to today.
        if t2 > datetime.today() - timedelta(days=1) or debug or not _cache:
            return self._select_between_impl(station, t1, t2, debug)
        return self._select_between_with_caching(station, t1, t2)

    @lru_cache(maxsize=50)
    def _select_between_with_caching(self, station, t1, t2):
        return self._select_between_impl(station, t1, t2)

    def _select_between_impl(self, station, t1, t2, debug=False):
        one_hr = np.timedelta64(1, 'h')
        querytimes = np.arange(t1, t2 + one_hr, one_hr)

        keys = []
        for querytime in querytimes:
            prefix = '{dt:%Y}/{dt:%m}/{dt:%d}/{st}/{st}{dt:%Y%m%d_%H}'.format(
                dt=querytime.astype(datetime), st=station)

            for key in self._bucket.list(prefix=prefix):
                try:
                    key_datetime = timestamp_from_filename(key.name)
                except ValueError as e:
                    print(str(e))
                    continue

                if t1 <= key_datetime < t2:
                    log_if_debug('Found key: {}'.format(key.name), debug)
                    keys.append(key)
                else:
                    log_if_debug('Skipped key: {}'.format(key.name), debug)

        return OrderSelection(keys)


_DT_REGEX = r'\d{8}_\d{6}'


def timestamp_from_filename(filename):
    # e.g. KVNX20120414_192456
    found_datetime = re.search(_DT_REGEX, filename)
    if found_datetime:
        return datetime.strptime(found_datetime.group(0), '%Y%m%d_%H%M%S')
    else:
        raise ValueError("Cannot find timestamp from key: {}".format(filename))


class OrderSelection(object):
    def __init__(self, keys):
        self._keys = keys

    @property
    def items(self):
        return [key.name for key in self._keys]

    def __bool__(self):
        return bool(self._keys)

    def __getitem__(self, item):
        sublist = self._keys[item]
        if not isinstance(sublist, list):
            sublist = [sublist]
        return OrderSelection(sublist)

    def download(self, dest=None, overwrite=False):
        if dest is None:
            dest = workdir.subdir('radar')

        downloaded_files = []
        for key in self._keys:
            filename = key.name.split('/')[-1]
            targ = os.path.join(dest, filename)

            if overwrite or not os.path.isfile(targ):
                try:
                    key.get_contents_to_filename(targ)
                except Exception:
                    import warnings
                    warnings.warn('Failed to save key: {} to disk: {}'.format(key.name, targ))
                    continue

            downloaded_files.append(targ)
        return downloaded_files


_DEFAULT_BOUNDS = {
    'reflectivity': (5, 75),
    'velocity': (-45, 45),
    'corrected_velocity': (-45, 45),
}

_OVERRIDE_DEFAULT_CM = {
    'velocity': 'pyart_NWSVel',
    'corrected_velocity': 'pyart_NWSVel',
}


def get_radar_and_display(file_or_radar):
    if isinstance(file_or_radar, pyart.core.Radar):
        sample = file_or_radar
    else:
        sample = pyart.io.read_nexrad_archive(file_or_radar)

    display = pyart.graph.RadarMapDisplay(sample)
    return sample, display


def plot_reflectivity(file_or_radar, sweep=0, **plot_kw):
    radarsample, display = get_radar_and_display(file_or_radar)
    plot_default_display(display, 'reflectivity', sweep, **plot_kw)
    return radarsample, display


def plot_velocity(file_or_radar, sweep=1, correct=True, **plot_kw):
    radarsample, display = get_radar_and_display(file_or_radar)
    if correct:
        dealiased = pyart.correct.dealias_region_based(radarsample, keep_original=True)
        radarsample.add_field('corrected_velocity', dealiased, replace_existing=True)
        plot_default_display(display, 'corrected_velocity', sweep, **plot_kw)
    else:
        plot_default_display(display, 'velocity', sweep, **plot_kw)
    return radarsample, display


def plot_default_display(display, field, sweep, vbounds=None, resolution='i',
                         zoom_km=None, shift_latlon=(0, 0), ctr_latlon=None,
                         bbox=(None, None, None, None),
                         map_bg_color='white', map_layer_color='k',
                         map_layers=('coastlines', 'countries', 'states', 'counties', 'highways'),
                         cmap=None, debug=False, basemap=None, ax=None):

    vmin, vmax = vbounds if vbounds is not None else _DEFAULT_BOUNDS.get(field, (None, None))
    cmap = cmap if cmap is not None else _OVERRIDE_DEFAULT_CM.get(field, None)

    if basemap is not None:
        geog_kw = dict(basemap=basemap)
        if ax is not None:
            # this is a hack to account for pyart's `plot_ppi_map` method always plotting
            # on the basemap's axes instance instead of the custom axes instance
            basemap.ax = ax
    elif zoom_km is not None:
        if ctr_latlon is None:
            log_if_debug('Center: {}, shift: {}'.format(display.loc, shift_latlon), debug)
            latctr, lonctr = tuple(map(sum, zip(display.loc, shift_latlon)))
        else:
            log_if_debug('Center: {}, shift: {}'.format(ctr_latlon, (0, 0)), debug)
            latctr, lonctr = ctr_latlon

        if not isinstance(zoom_km, (list, tuple)):
            zoom_km = (zoom_km, zoom_km)

        zoomwidth, zoomheight = zoom_km
        geog_kw = dict(width=zoomwidth * 2 * 1000, height=zoomheight * 2 * 1000,
                       lon_0=lonctr, lat_0=latctr)
        log_if_debug('width: {}, height: {}'.format(geog_kw['width'], geog_kw['height']), debug)
    else:
        lon0, lon1, lat0, lat1 = bbox
        geog_kw = dict(min_lon=lon0, min_lat=lat0, max_lon=lon1, max_lat=lat1)

    display.plot_ppi_map(field, sweep=sweep, title_flag=False, cmap=cmap,
                         vmin=vmin, vmax=vmax, resolution=resolution,
                         embelish=False, colorbar_flag=False, ax=ax,
                         **geog_kw)

    display.basemap.drawmapboundary(fill_color=map_bg_color)
    if 'coastlines' in map_layers:
        display.basemap.drawcoastlines(ax=ax, color=map_layer_color)
    if 'countries' in map_layers:
        display.basemap.drawcountries(ax=ax, color=map_layer_color)
    if 'states' in map_layers:
        display.basemap.drawstates(ax=ax, color=map_layer_color)
    if 'counties' in map_layers:
        display.basemap.drawcounties(ax=ax, color=map_layer_color, linewidth=0.15)
    if 'highways' in map_layers:
        draw_hways(display.basemap, ax=ax)