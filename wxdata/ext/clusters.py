import cartopy.crs as ccrs
import cartopy.feature as cfeat
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import MultiPoint
from sklearn.cluster import DBSCAN


def temporal_discretize(torn_seg, spacing_min=1):
    elapsed_min = (torn_seg.end_date_time - torn_seg.begin_date_time) / pd.Timedelta('1 min')
    slat, slon, elat, elon = torn_seg.begin_lat, torn_seg.begin_lon, torn_seg.end_lat, torn_seg.end_lon

    if any(np.isnan(pt) for pt in (slat, slon, elat, elon)):
        return np.empty((2, 4))

    numpoints = elapsed_min // spacing_min + 1
    lat_space = np.linspace(slat, elat, numpoints)
    lon_space = np.linspace(slon, elon, numpoints)

    return np.vstack([lat_space, lon_space]).T


def torn_latlons(torndata):
    tornref = _LatLonKeyMap()
    paths = []

    for _, torn in torndata.iterrows():
        tornpts = temporal_discretize(torn)
        for pt in tornpts:
            if pt not in tornref:
                tornref[pt] = []
            tornref[pt].append(torn)
        paths.append(tornpts)

    latlons = np.concatenate(paths)
    return latlons, tornref


def dbscan_clusters(latlons, weights=None, eps_mi=30, threshold=2.5):
    kms_per_radian = 6371.0088
    epsilon = eps_mi * 1.609 / kms_per_radian

    db = DBSCAN(eps=epsilon,
                min_samples=threshold,
                algorithm='ball_tree',
                metric='haversine').fit(np.radians(latlons),
                                        sample_weight=weights)

    cluster_labels = set(label for label in db.labels_ if label >= 0)
    outlier_label = -1
    clusters = pd.Series([latlons[db.labels_ == n] for n in cluster_labels])
    outliers = latlons[db.labels_ == outlier_label]

    return clusters, outliers


def centroid(cluster):
    if cluster.size == 0:
        raise ValueError("Cannot find centroid of empty cluster")
    ctr = MultiPoint(cluster).centroid
    return ctr.x, ctr.y


class _LatLonKeyMap(object):
    def __init__(self):
        self._entries = {}

    def __contains__(self, latlon):
        return latlon.tostring() in self._entries

    def __setitem__(self, latlon, index):
        self._entries[latlon.tostring()] = index

    def __getitem__(self, latlon):
        return self._entries[latlon.tostring()]


def plot_clusters(clusts, outliers, tornref, outline):
    fig = plt.figure(figsize=(16, 16), dpi=80)
    proj = ccrs.PlateCarree()
    ax = plt.axes(projection=proj)

    # Southern Plains
    # ax.set_extent((-107.5, -91, 27.5, 39.5))

    #     Central Plains
    #     ax.set_extent((-107.5, -91, 33.5, 44))

    # southeast
    #     ax.set_extent((-98, -74, 23, 40))

    ax.set_extent(outline)

    ax.coastlines()
    states = cfeat.NaturalEarthFeature(category='cultural', name='admin_1_states_provinces_lakes',
                                       scale='50m', facecolor='none')
    ax.add_feature(states, edgecolor='black', linewidth=1.0)

    colors = cm.rainbow(np.linspace(0, 1, len(clusts)))

    for cluster, color in zip(clusts, colors):
        ctrlon, ctrlat = centroid(cluster)
        ax.plot(ctrlat, ctrlon, 'o', markersize=30, transform=ccrs.Geodetic(), color=color, alpha=0.3)
        for pt in cluster:
            if pt in tornref:
                ax.plot(pt[1], pt[0], 'o', markersize=3, transform=ccrs.Geodetic(), color=color)
            else:
                ax.plot(pt[1], pt[0], '+', markersize=2, transform=ccrs.Geodetic(), color=color)

    for pt in outliers:
        if pt in tornref:
            ax.plot(pt[1], pt[0], 'o', markersize=3, transform=ccrs.Geodetic(), color='gray')
        else:
            ax.plot(pt[1], pt[0], '+', markersize=2, transform=ccrs.Geodetic(), color='gray')

    return fig
