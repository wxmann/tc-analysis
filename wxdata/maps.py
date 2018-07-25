from functools import partial

from mpl_toolkits.basemap import Basemap

from wxdata.config import get_resource


def north_america(resolution='l', ax=None, draw=None):
    m = Basemap(projection='lcc', resolution=resolution, ax=ax,
                lat_0=50, lon_0=-100, width=13000000, height=9300000,
                area_thresh=1000)

    if draw is None:
        draw = ['coastlines', 'countries', 'states']

    _draw_in_basemap(m, draw)
    return m


def conus(resolution='l', ax=None, draw=None):
    m = Basemap(projection='merc', resolution=resolution, ax=ax,
                llcrnrlon=-130, llcrnrlat=21, urcrnrlon=-64, urcrnrlat=53,
                area_thresh=1000)

    if draw is None:
        draw = ['coastlines', 'countries', 'states']

    _draw_in_basemap(m, draw)
    return m


def nhem(resolution='l', ax=None, draw=None):
    m = Basemap(projection='npstere', resolution=resolution, ax=ax,
                boundinglat=15, lon_0=-100,
                area_thresh=1000)

    if draw is None:
        draw = ['coastlines', 'countries', 'states']

    _draw_in_basemap(m, draw)
    return m


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

    _draw_in_basemap(m, draw)
    return m


great_plains = partial(simple_basemap, bbox=(-110, -88, 27.5, 50))
srn_plains = partial(simple_basemap, bbox=(-110, -88, 27.5, 40))


def _draw_in_basemap(basemap, layers):
    if layers == 'none':
        return

    if 'coastlines' in layers:
        basemap.drawcoastlines()
    if 'countries' in layers:
        basemap.drawcountries()
    if 'states' in layers:
        basemap.drawstates()
    if 'counties' in layers:
        basemap.drawcounties()
    if 'highways' in layers:
        draw_hways(basemap)


def draw_hways(basemap, color='red', linewidth=0.4, ax=None):
    basemap.readshapefile(get_resource('hways/hways'), 'hways', drawbounds=True,
                          color=color, linewidth=linewidth, ax=ax)