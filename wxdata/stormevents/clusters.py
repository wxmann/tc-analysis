import numpy as np
import pandas as pd
from pandas.core.common import SettingWithCopyWarning
from sklearn.cluster import DBSCAN

import itertools
import warnings

from wxdata.stormevents.tornprocessing import discretize, ef, longevity

_NOISE_LABEL = -1


def spatial_clusters(points, eps_km, min_samples):
    assert min_samples > 0
    kms_per_radian = 6371.0088
    dbscan_spatial = DBSCAN(eps=eps_km / kms_per_radian, metric='haversine',
                            algorithm='ball_tree', min_samples=min_samples)
    dataset_spatial = np.radians(points[['lat', 'lon']])
    return dbscan_spatial.fit_predict(dataset_spatial)


def temporal_clusters(points, eps_min, min_samples):
    assert min_samples > 0
    nanos_per_min = 10 ** 9 * 60
    dbscan_temporal = DBSCAN(eps=eps_min * nanos_per_min, metric='euclidean',
                             min_samples=min_samples)
    dataset_temporal = points['timestamp'].astype(np.int64).values.reshape(-1, 1)
    return dbscan_temporal.fit_predict(dataset_temporal)


def _intermed_st_clusters(data, eps_km, eps_min, min_samples):
    data['spatial'] = spatial_clusters(data, eps_km, min_samples)
    data['temporal'] = temporal_clusters(data, eps_min, min_samples)
    return data.groupby(['spatial', 'temporal'])


def st_clusters(events, eps_km, eps_min, min_samples):
    assert min_samples > 0
    points = discretize(events)

    label_gen = itertools.count()
    noises = []
    result_clusts = []

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=SettingWithCopyWarning)

        def extract_clusts(data):
            clusts = _intermed_st_clusters(data, eps_km, eps_min, min_samples)
            if len(clusts) == 1:
                indices, clust = next(iter(clusts))
                clust.drop(['spatial', 'temporal'], axis=1, inplace=True)

                if _NOISE_LABEL in indices:
                    clust['cluster'] = _NOISE_LABEL
                    noises.append(clust)
                else:
                    label = next(label_gen)
                    clust['cluster'] = label
                    result_clusts.append(Cluster(label, clust, events))
            else:
                for indices, clust in clusts:
                    if _NOISE_LABEL in indices:
                        clust.drop(['spatial', 'temporal'], axis=1, inplace=True)
                        clust['cluster'] = _NOISE_LABEL
                        noises.append(clust)
                    else:
                        extract_clusts(clust)

        extract_clusts(points)

    noise_pts = pd.concat(noises, ignore_index=True)
    return result_clusts, Cluster(_NOISE_LABEL, noise_pts, events)


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

    def tornado_stats(self):
        all_events = self.events
        tor_events = all_events[all_events.event_type == 'Tornado']
        tor_ef = ef(tor_events)
        tor_longevity = longevity(tor_events)

        ret = {'ef{}'.format(i): tor_ef[tor_ef == i].count() for i in range(0, 6)}
        ret['ef?'] = tor_ef[tor_ef.isnull()].count()
        ret['segments'] = len(tor_events)
        ret['total_time'] = tor_longevity.sum()

        return ret

    def plot(self, basemap, markersize=2, color=None, **kwargs):
        lons = self.latlons[:, 1]
        lats = self.latlons[:, 0]
        x, y = basemap(lons, lats)
        basemap.plot(x, y, 'o', markersize=markersize, color=color, **kwargs)
