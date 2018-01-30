import os
import re

from boto.s3.connection import S3Connection

from datetime import datetime, timedelta

from wxdata import workdir, geog
from wxdata.plotting import draw_hways
from wxdata.utils import log_if_debug

__all__ = ['OrderLevel2', 'OrderSelection', 'timestamp_from_key', 'plot_level2']


class OrderLevel2(object):
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
            dest = os.path.join(workdir.get(), 'radar')

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


def plot_level2(file_or_radar, field='reflectivity', sweep=0, bounds=None, resolution='i',
                zoom_km=None, shift_latlon=(0, 0), ctr_latlon=None, bbox=(None, None, None, None),
                map_layers=('states', 'counties', 'highways'),
                debug=False, ax=None):
    import pyart
    if isinstance(file_or_radar, pyart.core.Radar):
        radarsample = file_or_radar
    else:
        radarsample = pyart.io.read_nexrad_archive(file_or_radar)

    display = pyart.graph.RadarMapDisplay(radarsample)
    vmin, vmax = bounds if bounds is not None else _DEFAULT_BOUNDS.get(field, (None, None))

    if zoom_km is not None:
        log_if_debug('Radar location: {}, shift: {}'.format(display.loc, shift_latlon), debug)
        if ctr_latlon is None:
            latctr, lonctr = tuple(map(sum, zip(display.loc, shift_latlon)))
        else:
            latctr, lonctr = ctr_latlon

        if isinstance(zoom_km, int):
            zoom_km = (zoom_km, zoom_km)
        bbox = geog.bbox_zoom((latctr, lonctr), km_x=zoom_km[0], km_y=zoom_km[1])
        log_if_debug('BBox: {}'.format(bbox), debug)

    lon0, lon1, lat0, lat1 = bbox
    display.plot_ppi_map(field, sweep=sweep, title_flag=False,
                         min_lon=lon0, min_lat=lat0, max_lon=lon1, max_lat=lat1,
                         vmin=vmin, vmax=vmax, resolution=resolution,
                         embelish=False, colorbar_flag=False, ax=ax)

    if 'states' in map_layers:
        display.basemap.drawstates()
    if 'counties' in map_layers:
        display.basemap.drawcounties()
    if 'highways' in map_layers:
        draw_hways(display.basemap)

    return radarsample, display
