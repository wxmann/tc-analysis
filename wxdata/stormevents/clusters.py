import numpy as np
import pandas as pd
from pandas.core.common import SettingWithCopyWarning
from sklearn.cluster import DBSCAN
from sklearn.metrics import pairwise_distances

import itertools
import warnings


def spatial_clusters(df, eps_km, min_samples):
    kms_per_radian = 6371.0088
    dbscan_spatial = DBSCAN(eps=eps_km / kms_per_radian, metric='haversine', min_samples=min_samples)
    dataset_spatial = np.radians(df[['lat', 'lon']])
    return dbscan_spatial.fit(dataset_spatial)


def temporal_clusters(df, eps_min, min_samples):
    nanos_per_min = 10 ** 9 * 60
    dbscan_temporal = DBSCAN(eps=eps_min * nanos_per_min, metric='precomputed', min_samples=min_samples)
    times_nano = df['timestamp'].astype(np.int64).values.reshape(-1, 1)
    dataset_temporal = pairwise_distances(times_nano, metric=lambda x, y: abs(x - y))
    return dbscan_temporal.fit(dataset_temporal)


def st_clusters(df, eps_km, eps_min, min_samples, show_components=False):
    label_gen = itertools.count()

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=SettingWithCopyWarning)

        with_spatial = df.copy()
        with_spatial['spatial'] = spatial_clusters(df, eps_km, min_samples).labels_

        result_pieces = []
        for clust_label, clust in with_spatial.groupby('spatial'):
            clust['temporal'] = temporal_clusters(clust, eps_min, min_samples).labels_

            for indices, group in clust.groupby(['spatial', 'temporal']):
                if any(index < 0 for index in indices):
                    group['cluster'] = -1
                else:
                    group['cluster'] = next(label_gen)
                result_pieces.append(group)

    ret = pd.concat(result_pieces, ignore_index=True)
    if not show_components:
        ret.drop(['spatial', 'temporal'], axis=1, inplace=True)

    return ret
