import os

import numpy as np
import pandas as pd
import xarray as xr

from wxdata import stormevents, _timezones as _tz
from wxdata.extras import assert_clusters_equal, st_clusters, lat_weighted_spread
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


def test_lat_weighted_spread():
    ds = xr.open_dataarray(resource_path('gfs_ens_init_18090500_valid_18091300.nc'))
    ens_sd = ds.std('ens')
    spread_act = lat_weighted_spread(ens_sd, 'hgtprs', reducer=np.median)

    spread_exp = np.load(resource_path('lat_weighted_spread_expected.npy'))
    assert np.allclose(spread_act, spread_exp)