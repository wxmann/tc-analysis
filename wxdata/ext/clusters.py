import matplotlib.cm as cm
import numpy as np
import pandas as pd
from shapely.geometry import MultiPoint
from sklearn.cluster import DBSCAN

from wxdata import map_plotting


def temporal_discretize(torn_seg, spacing_min=1):
    elapsed_min = (torn_seg.end_date_time - torn_seg.begin_date_time) / pd.Timedelta('1 min')
    slat, slon, elat, elon = torn_seg.begin_lat, torn_seg.begin_lon, torn_seg.end_lat, torn_seg.end_lon

    numpoints = elapsed_min // spacing_min + 1
    lat_space = np.linspace(slat, elat, numpoints)
    lon_space = np.linspace(slon, elon, numpoints)

    return np.vstack([lat_space, lon_space]).T


def torn_latlons(torndata):
    tornref = {}

    for _, torn in torndata.iterrows():
        tornpts = temporal_discretize(torn)
        for pt in tornpts:
            if pt not in tornref:
                tornref[pt] = []
            tornref[pt].append(torn)

    latlons = np.concatenate(tornref.keys())
    return latlons, tornref


def spatial_clusters(latlons, weights=None, eps_mi=30, threshold=2.5):
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


def plot_clusters(clusters, outliers, cluster_colors=None, mapper=None):
    if mapper is None:
        mapper = map_plotting.get_map()
    if cluster_colors is None:
        cluster_colors = cm.rainbow(np.linspace(0, 1, len(clusters)))
    elif len(cluster_colors) != len(clusters):
        raise ValueError("Size of cluster colors != size of clusters")

    for cluster, color in zip(clusters, cluster_colors):
        for pt in cluster:
            lat, lon = reversed(pt)
            map_plotting.plot_point(mapper, lat, lon, 'o', markersize=2, color=color)

    for pt in outliers:
        lat, lon = reversed(pt)
        map_plotting.plot_point(mapper, lat, lon, '+', markersize=2, color='gray')


def plot_centroids(clusters, cluster_colors=None, mapper=None):
    if mapper is None:
        mapper = map_plotting.get_map()
    if cluster_colors is None:
        cluster_colors = cm.rainbow(np.linspace(0, 1, len(clusters)))
    elif len(cluster_colors) != len(clusters):
        raise ValueError("Size of cluster colors != size of clusters")

    for cluster, color in zip(clusters, cluster_colors):
        ctrlon, ctrlat = centroid(cluster)
        map_plotting.plot_point(mapper, ctrlat, ctrlon, 'o', markersize=30, color=color, alpha=0.3)