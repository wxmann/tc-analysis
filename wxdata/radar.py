import os
import re

from boto.s3.connection import S3Connection
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
        if isinstance(timestamp, str):
            timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M')

        if isinstance(dt, int):
            dt = timedelta(minutes=dt)

        min_time = timestamp - dt
        max_time = timestamp + dt

        keys = []
        querytimes = (min_time,) if min_time.hour == max_time.hour else (min_time, max_time)
        log_if_debug('Time range considered: {}'.format(querytimes), debug)

        for querytime in querytimes:
            prefix = '{dt:%Y}/{dt:%m}/{dt:%d}/{st}/{st}{dt:%Y%m%d_%H}'.format(
                dt=querytime, st=station)

            for key in self._bucket.list(prefix=prefix):
                try:
                    key_datetime = timestamp_from_filename(key.name)
                except ValueError as e:
                    print(str(e))
                    continue

                t_hi = max(key_datetime, timestamp)
                t_lo = key_datetime if t_hi is timestamp else timestamp

                if t_hi - t_lo < dt:
                    log_if_debug('Found key: {}'.format(key.name), debug)
                    keys.append(key)
                else:
                    log_if_debug('Skipped key: {}'.format(key.name), debug)

        return OrderSelection(keys)

    def select_hours(self, station, order_date, from_hr=0, to_hr=24):
        if isinstance(order_date, str):
            order_date = datetime.strptime(order_date, '%Y-%m-%d')

        keys = []
        for hr in range(from_hr, to_hr):
            prefix = '{dt:%Y}/{dt:%m}/{dt:%d}/{st}/{st}{dt:%Y%m%d}_{hr}'.format(
                dt=order_date, st=station, hr=str(hr).zfill(2))
            keys += list(self._bucket.list(prefix=prefix))

        return OrderSelection(keys)


DT_REGEX = r'\d{8}_\d{6}'


def timestamp_from_filename(filename):
    # e.g. KVNX20120414_192456
    found_datetime = re.search(DT_REGEX, filename)
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
                key.get_contents_to_filename(targ)

            downloaded_files.append(targ)
        return downloaded_files


_DEFAULT_BOUNDS = {
    'reflectivity': (0, 75)
}

_OVERRIDE_DEFAULT_CM = {
    'velocity': 'pyart_NWSVel',
    'corrected_velocity': 'pyart_NWSVel',
    'simulated_velocity': 'pyart_NWSVel'
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

        if isinstance(zoom_km, int):
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

    if 'coastlines' in map_layers:
        display.basemap.drawcoastlines(ax=ax)
    if 'countries' in map_layers:
        display.basemap.drawcountries(ax=ax)
    if 'states' in map_layers:
        display.basemap.drawstates(ax=ax)
    if 'counties' in map_layers:
        display.basemap.drawcounties(ax=ax)
    if 'highways' in map_layers:
        draw_hways(display.basemap, ax=ax)