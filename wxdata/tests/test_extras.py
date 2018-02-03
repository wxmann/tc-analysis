import os

import pandas as pd

from wxdata import stormevents, _timezones as _tz
from wxdata.extras import assert_clusters_equal, st_clusters
from wxdata.extras.clusters import Cluster, NOISE_LABEL
from wxdata.testing import resource_path


def test_find_st_clusters():
    df = stormevents.load_file(resource_path('120414_tornadoes.csv'), tz_localize=True)

    expected_torclusters = []
    expected_outliers = pd.DataFrame()

    clusterfile_dir = resource_path('120414_clusters')
    for clusterfile in os.listdir(clusterfile_dir):
        clusterfile = os.path.join(clusterfile_dir, clusterfile)
        clusterpts = pd.read_csv(clusterfile, parse_dates=['timestamp'])
        # hard-code this for now
        clusterpts['timestamp'] = clusterpts.apply(
            lambda r: r['timestamp'].tz_localize('GMT').tz_convert(_tz.parse_tz('CST')), axis=1)
        cluster_num = clusterpts.loc[0, 'cluster']
        cluster = Cluster(cluster_num, clusterpts, df)

        if cluster_num == NOISE_LABEL:
            expected_outliers = cluster
        else:
            expected_torclusters.append(cluster)

    expected_torclusters = sorted(expected_torclusters,
                                  key=lambda cl: (cl.begin_time, cl.end_time, len(cl)))

    result = st_clusters(df, 60, 60, 15)
    actual_torclusters = result.clusters
    actual_outliers = result.noise

    actual_torclusters = sorted(actual_torclusters,
                                key=lambda cl: (cl.begin_time, cl.end_time, len(cl)))

    assert len(expected_torclusters) == len(actual_torclusters)
    assert_clusters_equal(actual_outliers, expected_outliers)
    for actual, expected in zip(actual_torclusters, expected_torclusters):
        assert_clusters_equal(actual, expected)