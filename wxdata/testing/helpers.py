import os

from pandas.util.testing import assert_frame_equal


def resource_path(filename):
    this_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(this_dir, 'resources', filename)


def open_resource(filename, *args, **kwargs):
    return open(resource_path(filename), *args, **kwargs)


def assert_frame_eq_ignoring_dtypes(df1, df2,
                                    dt_columns=('begin_date_time', 'end_date_time')):
    assert_frame_equal(df1, df2, check_dtype=False)
    # the assert_frame_equal function doesn't work with localized vs. naive timestamps.
    # so we need to do another check for datetime columns
    for col in dt_columns:
        if col in df1.columns:
            assert df1[col].equals(df2[col])