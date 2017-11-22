import os
from itertools import product
from unittest import mock

import numpy as np
import pandas as pd
import pytz
from pandas.util.testing import assert_frame_equal, assert_series_equal

from wxdata import stormevents, _timezones, workdir
from wxdata.stormevents import urls_for, convert_timestamp_tz


def resource_path(filename):
    this_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(this_dir, 'resources', filename)


def open_resource(filename, *args, **kwargs):
    return open(resource_path(filename), *args, **kwargs)


@mock.patch('wxdata.common.requests')
def test_urls_for(req):
    response = mock.MagicMock()
    req.get.return_value = response

    response.status_code = 200
    with open_resource('mock_ncdc_listing.html', 'r') as f:
        text = f.read()
        response.text = text

    results = urls_for([1952, 1954, 2005])
    expected_urls = ['https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/{}'.format(file) for file in (
        'StormEvents_details-ftp_v1.0_d1952_c20170619.csv.gz',
        'StormEvents_details-ftp_v1.0_d1954_c20160223.csv.gz'
    )]
    assert results == expected_urls


def test_convert_timestamp_tz():
    original_tzs = ('CST-6', 'CST', 'MDT', 'Etc/GMT+6')
    target_tzs = ('GMT', 'UTC')

    for original_tz, target_tz in product(original_tzs, target_tzs):
        # our time to GMT
        assert convert_timestamp_tz('2017-01-01 00:00',
                                    original_tz, target_tz) == pd.Timestamp('2017-01-01 06:00', tz='GMT')
        assert convert_timestamp_tz('2016-12-31 18:00',
                                    original_tz, target_tz) == pd.Timestamp('2017-01-01 00:00', tz='GMT')

        # GMT to our time
        assert convert_timestamp_tz('2017-01-01 06:00',
                                    target_tz, original_tz) == pd.Timestamp('2017-01-01 00:00', tz='Etc/GMT+6')

        # converting timestamp object
        assert convert_timestamp_tz(pd.Timestamp('2017-01-01 00:00'),
                                    original_tz, target_tz) == pd.Timestamp('2017-01-01 06:00', tz='GMT')

        # converting datetime object
        from datetime import datetime
        assert convert_timestamp_tz(datetime(2017, 1, 1, 0, 0),
                                    original_tz, target_tz) == pd.Timestamp('2017-01-01 06:00', tz='GMT')


@mock.patch('wxdata._timezones.tz_for_latlon')
def test_convert_df_timezone(latlontz):
    def handle_latlon_tz(lat, lon):
        if lat is None or lon is None:
            return None
        return pytz.timezone('Etc/GMT+5')

    latlontz.side_effect = handle_latlon_tz
    src_df = stormevents.load_file(resource_path('stormevents_mixed_tzs.csv'))
    src_df = src_df[['begin_yearmonth', 'begin_day', 'begin_time', 'end_yearmonth',
                     'end_day', 'end_time', 'state', 'year', 'month_name', 'event_type',
                     'cz_name', 'cz_timezone', 'begin_date_time', 'end_date_time',
                     'begin_lat', 'begin_lon', 'episode_narrative', 'event_narrative']]

    expected_df = _load_localizing_timezones(resource_path('stormevents_mixed_tzs_togmt.csv'))
    converted_src_df = stormevents.convert_df_tz(src_df, 'GMT')
    _assert_frame_eq_ignoring_index_and_dtypes(converted_src_df, expected_df)


@mock.patch('wxdata._timezones.tz_for_latlon')
def test_convert_df_timezone_multiple_times(latlontz):
    def handle_latlon_tz(lat, lon):
        if lat is None or lon is None:
            return None
        return pytz.timezone('Etc/GMT+5')

    latlontz.side_effect = handle_latlon_tz
    src_df = stormevents.load_file(resource_path('stormevents_mixed_tzs.csv'))
    src_df = src_df[['begin_yearmonth', 'begin_day', 'begin_time', 'end_yearmonth',
                     'end_day', 'end_time', 'state', 'year', 'month_name', 'event_type',
                     'cz_name', 'cz_timezone', 'begin_date_time', 'end_date_time',
                     'begin_lat', 'begin_lon', 'episode_narrative', 'event_narrative']]

    expected_df = _load_localizing_timezones(resource_path('stormevents_mixed_tzs_togmt.csv'))
    intermed = stormevents.convert_df_tz(src_df, 'CST')
    converted_src_df = stormevents.convert_df_tz(intermed, 'GMT')
    _assert_frame_eq_ignoring_index_and_dtypes(converted_src_df, expected_df)


def test_filter_df_stormtype():
    df = stormevents.load_file(resource_path('stormevents_mixed_tzs.csv'), eventtypes=['Tornado', 'Hail'])
    eventtypes = df[['event_type']]

    assert len(eventtypes) == 13
    assert len(eventtypes[eventtypes.event_type == 'Tornado']) == 12
    assert len(eventtypes[eventtypes.event_type == 'Hail']) == 1
    assert len(eventtypes[eventtypes.event_type == 'Invalid']) == 0


def test_filter_df_state():
    df = stormevents.load_file(resource_path('stormevents_mixed_tzs.csv'), states=['Hawaii', 'Colorado'])
    states = df[['state']]

    assert len(states) == 4
    assert len(states[states.state == 'HAWAII']) == 3
    assert len(states[states.state == 'COLORADO']) == 1
    assert len(states[states.state == 'NO']) == 0


@mock.patch('wxdata.common.get_links', return_value=(
    'StormEvents_details-ftp_v1.0_d1990_c20170717.csv.gz',
    'StormEvents_details-ftp_v1.0_d1991_c20170717.csv.gz',
    'StormEvents_details-ftp_v1.0_d1992_c20170717.csv.gz',
))
def test_load_multiple_years_storm_data(reqpatch):
    workdir.setto(resource_path(''))
    df = stormevents.load_events('1990-01-01', '1992-10-31', eventtypes=['Tornado'],
                                 states=['Texas', 'Oklahoma', 'Kansas'])

    df_expected = stormevents.load_file(resource_path('multiyear_storm_events_expected.csv'))
    _assert_frame_eq_ignoring_index_and_dtypes(df, df_expected)


@mock.patch('wxdata.common.get_links', return_value=(
    'StormEvents_details-ftp_v1.0_d1990_c20170717.csv.gz',
    'StormEvents_details-ftp_v1.0_d1991_c20170717.csv.gz',
    'StormEvents_details-ftp_v1.0_d1992_c20170717.csv.gz',
))
def test_load_multiple_years_storm_data_localize_to_tz(reqpatch):
    workdir.setto(resource_path(''))
    df = stormevents.load_events('1990-01-01', '1992-10-31', eventtypes=['Tornado'],
                                 states=['Texas', 'Oklahoma', 'Kansas'], tz='EST')

    df_expected = stormevents.load_file(resource_path('multiyear_storm_events_EST_expected.csv'))
    _assert_frame_eq_ignoring_index_and_dtypes(df, df_expected)


def test_correct_tornado_times():
    df = stormevents.load_file(resource_path('stormevents_bad_times.csv'))
    df = stormevents.tors.correct_tornado_times(df)

    df_expected = stormevents.load_file(resource_path('stormevents_bad_times_corrected.csv'))
    _assert_frame_eq_ignoring_index_and_dtypes(df, df_expected)


def test_after_tz_conversion_correct_tornado_times():
    df = stormevents.load_file(resource_path('stormevents_bad_times.csv'), tz='GMT')
    df = stormevents.tors.correct_tornado_times(df)

    df_expected = stormevents.load_file(resource_path('stormevents_bad_times_GMT_corrected.csv'))
    _assert_frame_eq_ignoring_index_and_dtypes(df, df_expected)


def test_get_longevity():
    init = pd.Timestamp('1990-01-01 00:00')
    deltas = [pd.Timedelta(hours=0), pd.Timedelta('00:01:11'), pd.Timedelta(hours=1), pd.Timedelta(minutes=45)]
    start_times = [init] * len(deltas)
    end_times = [init + dt for dt in deltas]

    df = pd.DataFrame({'begin_date_time': start_times, 'end_date_time': end_times})

    longevities = stormevents.tors.longevity(df)
    assert_series_equal(longevities, pd.Series(deltas))


def test_get_ef():
    entries = [f + str(rating) for f, rating in product(['F', 'EF'], range(6))]
    entries.append(np.nan)
    entries.append('')
    entries.append('NaN')
    entries.append('EFU')

    df = pd.DataFrame({'tor_f_scale': entries})

    efs = stormevents.tors.ef(df)

    expected = [float(f) for f in range(6)] * 2
    expected += [np.nan] * 4
    assert_series_equal(efs, pd.Series(expected), check_names=False)


def test_get_speed():
    init = pd.Timestamp('1990-01-01 00:00')
    deltas = [pd.Timedelta(hours=0), pd.Timedelta(minutes=30), pd.Timedelta(hours=1), pd.Timedelta(hours=2)]
    start_times = [init] * len(deltas)
    end_times = [init + dt for dt in deltas]
    tor_lengths = [10] * len(deltas)

    df = pd.DataFrame({'begin_date_time': start_times, 'end_date_time': end_times,
                       'tor_length': tor_lengths})

    speeds = stormevents.tors.speed_mph(df)
    assert_series_equal(speeds, pd.Series([np.nan, 20, 10, 5]))


def _load_localizing_timezones(file):
    df = stormevents.load_file(file)
    df['begin_date_time'] = df.apply(lambda row: _timezones.parse_tz(row['cz_timezone']).localize(row['begin_date_time']),
                                     axis=1)
    df['end_date_time'] = df.apply(lambda row: _timezones.parse_tz(row['cz_timezone']).localize(row['end_date_time']),
                                   axis=1)
    return df


def _assert_frame_eq_ignoring_index_and_dtypes(df1, df2):
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    assert_frame_equal(df1, df2, check_dtype=False)
