import numpy as np
import pandas as pd
from pandas.core.common import SettingWithCopyWarning
from sklearn.cluster import DBSCAN

import itertools
import warnings


def spatial_clusters(df, eps_km, min_samples):
    assert min_samples > 0
    kms_per_radian = 6371.0088
    dbscan_spatial = DBSCAN(eps=eps_km / kms_per_radian, metric='haversine',
                            algorithm='ball_tree', min_samples=min_samples)
    dataset_spatial = np.radians(df[['lat', 'lon']])
    return dbscan_spatial.fit_predict(dataset_spatial)


def temporal_clusters(df, eps_min, min_samples):
    assert min_samples > 0
    nanos_per_min = 10 ** 9 * 60
    dbscan_temporal = DBSCAN(eps=eps_min * nanos_per_min, metric='euclidean',
                             min_samples=min_samples)
    dataset_temporal = df['timestamp'].astype(np.int64).values.reshape(-1, 1)
    return dbscan_temporal.fit_predict(dataset_temporal)


def _intermed_st_clusters(clust, eps_km, eps_min):
    clust['spatial'] = spatial_clusters(clust, eps_km, 1)
    clust['temporal'] = temporal_clusters(clust, eps_min, 1)
    return clust.groupby(['spatial', 'temporal'])


def st_clusters(df, eps_km, eps_min, min_samples):
    assert min_samples > 0
    label_gen = itertools.count()
    pieces = []

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=SettingWithCopyWarning)

        def extract_clusts(data):
            clusts = _intermed_st_clusters(data, eps_km, eps_min)
            if len(clusts) == 1:
                _, clust = next(iter(clusts))
                clust.drop(['spatial', 'temporal'], axis=1, inplace=True)

                if clust.shape[0] < min_samples:
                    clust['cluster'] = -1
                else:
                    clust['cluster'] = next(label_gen)
                pieces.append(clust)
            else:
                for _, clust in clusts:
                    extract_clusts(clust)

        extract_clusts(df.copy())

    return pd.concat(pieces, ignore_index=True)
