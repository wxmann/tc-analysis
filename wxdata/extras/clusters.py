from functools import partial

import numpy as np
import pandas as pd
import matplotlib.patheffects as path_effects
from geopy.distance import great_circle
from pandas.util.testing import assert_frame_equal
from sklearn.cluster import DBSCAN
from sklearn.metrics import pairwise_distances

from wxdata.plotting import plot_points
from wxdata.stormevents.tornprocessing import discretize, ef, longevity

__all__ = ['st_clusters', 'plot_clusters', 'assert_clusters_equal']

NOISE_LABEL = -1


def st_clusters(events, eps_km, eps_min, min_samples, algorithm=None):
    assert min_samples > 0

    if events.empty:
        return ClusterGroup.empty()

    if algorithm == 'brute':
        cluster_dict = _brute_st_clusters(events, eps_km, eps_min, min_samples)
    else:
        points = discretize(events)
        points['timestamp_nanos'] = points.timestamp.astype(np.int64)
        n_jobs = 1 if len(points) < 100 else -1
        similarity = pairwise_distances(points[['lat', 'lon', 'timestamp_nanos']],
                                        metric=partial(_boolean_distance, eps_km=eps_km, eps_min=eps_min),
                                        n_jobs=n_jobs)

        db = DBSCAN(eps=0.5, metric='precomputed', min_samples=min_samples)
        cluster_labels = db.fit_predict(similarity)

        points.drop('timestamp_nanos', axis=1, inplace=True)
        points['cluster'] = cluster_labels

        cluster_dict = {label: Cluster(label, points[points.cluster == label], events)
                        for label in points.cluster.unique()}

    return ClusterGroup(cluster_dict)


def _boolean_distance(pt1, pt2, eps_km, eps_min,
                      lat_index=0, lon_index=1, timestamp_nanos_index=2):
    nanos_per_min = 10 ** 9 * 60
    return abs(pt1[timestamp_nanos_index] - pt2[timestamp_nanos_index]) > eps_min * nanos_per_min or \
           great_circle((pt1[lat_index], pt1[lon_index]), (pt2[lat_index], pt2[lon_index])).km > eps_km


## brute force clustering algorithm


def _brute_st_clusters(events, eps_km, eps_min, min_samples):
    points = discretize(events)
    noise = NOISE_LABEL
    undetermined = -999

    label = 0
    points['cluster'] = undetermined
    neighb_threshold = min_samples - 1
    clusterpts = set()
    noisepts = set()

    def is_noise(group, threshold):
        return group.shape[0] < threshold

    for index in points.index:
        if index in clusterpts or index in noisepts:
            continue

        neighb = _neighbors(points, eps_km, eps_min, index)
        if is_noise(neighb, neighb_threshold):
            points.set_value(index, 'cluster', noise)
            noisepts.add(index)
        else:
            label += 1
            points.set_value(index, 'cluster', label)
            clusterpts.add(index)
            subpts = {i for i in neighb.index if i not in clusterpts}

            while subpts:
                qindex = subpts.pop()
                if qindex in noisepts:
                    points.set_value(qindex, 'cluster', label)
                    noisepts.remove(qindex)
                    clusterpts.add(qindex)

                if qindex in clusterpts:
                    continue

                points.set_value(qindex, 'cluster', label)
                clusterpts.add(qindex)
                neighb_inner = _neighbors(points, eps_km, eps_min, qindex)

                if not is_noise(neighb_inner, neighb_threshold):
                    subpts = subpts.union({i for i in neighb_inner.index if i not in clusterpts})

    clusters = {}
    for clust_label in points.cluster.unique():
        clust_points = points[points.cluster == clust_label]
        clusters[clust_label] = Cluster(clust_label, clust_points, events)

    noise_points = points[points.cluster == noise]
    clusters[noise] = Cluster(noise, noise_points, events)
    return clusters


def _neighbors(df, spatial_dist, temporal_dist, index):
    pt = df.loc[index]
    tmin = pt.timestamp - pd.Timedelta(minutes=temporal_dist)
    tmax = pt.timestamp + pd.Timedelta(minutes=temporal_dist)

    filtered = df[(df.timestamp >= tmin) & (df.timestamp <= tmax)]
    mask = filtered.apply(lambda r: great_circle((r.lat, r.lon), (pt.lat, pt.lon)).km, axis=1) < spatial_dist
    return filtered[mask & (filtered.index != pt.name)]


## Objects


class ClusterGroup(object):

    @classmethod
    def empty(cls):
        return cls({})

    def __init__(self, cluster_dict):
        self._cluster_dict = cluster_dict

    def __getitem__(self, item):
        return self._cluster_dict[item]

    def __contains__(self, item):
        return item in self._cluster_dict

    def __len__(self):
        if NOISE_LABEL in self._cluster_dict:
            return len(self._cluster_dict) - 1
        else:
            return len(self._cluster_dict)

    def __bool__(self):
        return len(self) > 0

    @property
    def clusters(self):
        unordered = [clust for i, clust in self._cluster_dict.items() if i != NOISE_LABEL]
        return sorted(unordered, key=lambda cl: (cl.begin_time, cl.end_time, len(cl)))

    @property
    def noise(self):
        return self._cluster_dict[NOISE_LABEL]


class Cluster(object):
    def __init__(self, cluster_num, cluster_pts, parent):
        self._index = cluster_num
        self._points = cluster_pts
        self._parent = parent

    @property
    def index(self):
        return self._index

    @property
    def pts(self):
        return self._points.copy()

    @property
    def centroid(self):
        from shapely.geometry import MultiPoint
        ctr = MultiPoint(self.latlons).centroid
        return ctr.x, ctr.y

    @property
    def latlons(self):
        return self._points[['lat', 'lon']].as_matrix()

    @property
    def events(self):
        return self._parent[self._parent.event_id.isin(
            self._points[self._points.cluster == self._index].event_id.unique())]

    @property
    def begin_time(self):
        return self._points.timestamp.min()

    @property
    def end_time(self):
        return self._points.timestamp.max() + pd.Timedelta('1 min')

    def __len__(self):
        return len(self._points)

    def summary(self):
        ts = self._points.timestamp
        return {
            'min_time': ts.min(),
            'max_time': ts.max(),
            'size': len(self._points),
            'time_spread': ts.max() - ts.min(),
            'center': self.centroid
        }

    def describe_tors(self, show_index=False):
        if self._index == NOISE_LABEL:
            time_part = '(Outliers)'
        else:
            time_part = '{} to {}'.format(self.begin_time.strftime('%Y-%m-%d %H:%M'),
                                          self.end_time.strftime('%Y-%m-%d %H:%M'))
            if show_index:
                time_part = '({}) '.format(self.index) + time_part

        stats = self.tor_stats()

        ef_labels = ['EF{}'.format(f) for f in range(0, 6)]
        ef_parts = ['{}: {}'.format(label, stats[label.lower()]) for label in ef_labels]
        if stats['ef?']:
            ef_parts.append('EF?: {}'.format(stats['ef?']))

        segments_part = '{} segments ({})'.format(stats['segments'], ', '.join(ef_parts))
        casualty_part = '{} fatalities | {} injuries'.format(stats['fatalities'], stats['injuries'])

        return '\n'.join([time_part, segments_part, casualty_part])

    def tor_stats(self):
        all_events = self.events
        tor_events = all_events[all_events.event_type == 'Tornado']
        tor_ef = ef(tor_events)
        tor_longevity = longevity(tor_events)

        ret = {'ef{}'.format(i): tor_ef[tor_ef == i].count() for i in range(0, 6)}
        ret['ef?'] = tor_ef[tor_ef.isnull()].count()
        ret['segments'] = len(tor_events)
        ret['total_time'] = tor_longevity.sum()
        ret['fatalities'] = tor_events.deaths_direct.sum()
        ret['injuries'] = tor_events.injuries_direct.sum()

        return ret

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Cluster):
            return False

        try:
            assert_clusters_equal(self, other)
            return True
        except AssertionError:
            return False


## utilities

def assert_clusters_equal(clust1, clust2):
    clust1_pts = clust1.pts.copy()
    clust2_pts = clust2.pts.copy()
    clust1_pts.reset_index(drop=True, inplace=True)
    clust2_pts.reset_index(drop=True, inplace=True)
    clust1_pts.drop('cluster', inplace=True, axis=1)
    clust2_pts.drop('cluster', inplace=True, axis=1)

    assert_frame_equal(clust1_pts, clust2_pts)

    clust1_evts = clust1.events.copy()
    clust2_evts = clust2.events.copy()
    clust1_evts.reset_index(drop=True, inplace=True)
    clust2_evts.reset_index(drop=True, inplace=True)

    assert_frame_equal(clust1_evts, clust2_evts)


## plotting

# TODO: should we keep this function?

def plot_clusters(cluster_groups, basemap, cluster_colors, noise_color='gray',
                  legend=None):

    assert len(cluster_groups) == len(cluster_colors)
    shadow = path_effects.withSimplePatchShadow(offset=(1, -1), alpha=0.6)

    for clust in cluster_groups.clusters:
        color = cluster_colors[clust.index]
        plot_points(clust.pts, basemap, color, markersize=1.5, path_effects=[shadow])
        if legend:
            legend.append(color, clust.describe_tors())

    noise = cluster_groups.noise
    plot_points(noise.pts, basemap, markersize=1.5, color=noise_color, path_effects=[shadow])
    if legend:
        legend.append(noise_color, noise.describe_tors())