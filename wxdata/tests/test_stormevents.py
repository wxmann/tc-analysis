from itertools import product
from unittest import mock

import numpy as np
import pandas as pd
import pytz
from pandas.util.testing import assert_series_equal

from wxdata import stormevents, workdir
from wxdata.stormevents import urls_for, convert_timestamp_tz, localize_timestamp_tz
from wxdata.testing import resource_path, open_resource, assert_frame_eq_ignoring_dtypes


@mock.patch('wxdata.http.requests')
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
    cst_tzs = ('CST-6', 'CST', 'MDT', 'Etc/GMT+6')
    gmt_tzs = ('GMT', 'UTC')

    for tz_cst, tz_gmt in product(cst_tzs, gmt_tzs):
        # our time to GMT
        assert convert_timestamp_tz('2017-01-01 00:00',
                                    tz_cst, tz_gmt) == pd.Timestamp('2017-01-01 06:00', tz='GMT')
        assert convert_timestamp_tz('2016-12-31 18:00',
                                    tz_cst, tz_gmt) == pd.Timestamp('2017-01-01 00:00', tz='GMT')

        # GMT to our time
        assert convert_timestamp_tz('2017-01-01 06:00',
                                    tz_gmt, tz_cst) == pd.Timestamp('2017-01-01 00:00', tz='Etc/GMT+6')

        # converting timestamp object
        assert convert_timestamp_tz(pd.Timestamp('2017-01-01 00:00'),
                                    tz_cst, tz_gmt) == pd.Timestamp('2017-01-01 06:00', tz='GMT')

        # converting datetime object
        from datetime import datetime
        assert convert_timestamp_tz(datetime(2017, 1, 1, 0, 0),
                                    tz_cst, tz_gmt) == pd.Timestamp('2017-01-01 06:00', tz='GMT')

        # converting already tz-localized object
        assert convert_timestamp_tz('2017-01-01 00:00-00',
                                    tz_cst, tz_gmt) == pd.Timestamp('2017-01-01 00:00', tz='GMT')

        # converting to same timezone
        assert convert_timestamp_tz('2017-01-01 18:00-06',
                                    tz_cst, tz_cst) == pd.Timestamp('2017-01-01 18:00', tz='Etc/GMT+6')

        assert convert_timestamp_tz(pd.Timestamp('2017-01-01 00:00-00'),
                                    tz_cst, tz_gmt) == pd.Timestamp('2017-01-01 00:00', tz='GMT')


def test_localize_timestamp_tz():
    assert localize_timestamp_tz(pd.Timestamp('2017-01-01 18:00-00'),
                                 'CST') == pd.Timestamp('2017-01-01 12:00', tz='Etc/GMT+6')

    assert localize_timestamp_tz(pd.Timestamp('2017-01-01 18:00'),
                                 'CST') == pd.Timestamp('2017-01-01 18:00', tz='Etc/GMT+6')

    assert localize_timestamp_tz('2017-01-01 18:00',
                                 'CST') == pd.Timestamp('2017-01-01 18:00', tz='Etc/GMT+6')

    # doesn't mess up an already localized timestamp
    assert localize_timestamp_tz(pd.Timestamp('2017-01-01 18:00-06'),
                                 'CST') == pd.Timestamp('2017-01-01 18:00', tz='Etc/GMT+6')


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

    expected_df = stormevents.load_file(resource_path('stormevents_mixed_tzs_togmt.csv'), tz_localize=True)
    converted_src_df = stormevents.convert_df_tz(src_df, 'GMT')
    assert_frame_eq_ignoring_dtypes(converted_src_df, expected_df)


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

    expected_df = stormevents.load_file(resource_path('stormevents_mixed_tzs_togmt.csv'), tz_localize=True)
    intermed = stormevents.convert_df_tz(src_df, 'CST')
    converted_src_df = stormevents.convert_df_tz(intermed, 'GMT')
    assert_frame_eq_ignoring_dtypes(converted_src_df, expected_df)


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


@mock.patch('wxdata.http.get_links', return_value=(
        'StormEvents_details-ftp_v1.0_d1990_c20170717.csv.gz',
        'StormEvents_details-ftp_v1.0_d1991_c20170717.csv.gz',
        'StormEvents_details-ftp_v1.0_d1992_c20170717.csv.gz',
))
def test_load_multiple_years_storm_data(reqpatch):
    workdir.setto(resource_path(''))
    df = stormevents.load_events('1990-01-01', '1992-10-31', eventtypes=['Tornado'],
                                 states=['Texas', 'Oklahoma', 'Kansas'])

    df_expected = stormevents.load_file(resource_path('multiyear_storm_events_expected.csv'))
    assert_frame_eq_ignoring_dtypes(df, df_expected)


@mock.patch('wxdata.http.get_links', return_value=(
        'StormEvents_details-ftp_v1.0_d1990_c20170717.csv.gz',
        'StormEvents_details-ftp_v1.0_d1991_c20170717.csv.gz',
        'StormEvents_details-ftp_v1.0_d1992_c20170717.csv.gz',
))
def test_load_multiple_years_storm_data_localize_to_tz(reqpatch):
    workdir.setto(resource_path(''))
    df = stormevents.load_events('1990-01-01', '1992-10-31', eventtypes=['Tornado'],
                                 states=['Texas', 'Oklahoma', 'Kansas'], tz='EST')

    df_expected = stormevents.load_file(resource_path('multiyear_storm_events_EST_expected.csv'),
                                        tz_localize=True)
    assert_frame_eq_ignoring_dtypes(df, df_expected)


@mock.patch('wxdata.http.get_links', return_value=(
        'StormEvents_details-ftp_v1.0_d1991_c20170717.csv.gz',
))
def test_load_two_days_storm_data_localize_to_tz(reqpatch):
    workdir.setto(resource_path(''))
    df = stormevents.load_events('1991-04-26 12:00', '1991-04-28 12:00', eventtypes=['Tornado'], tz='UTC')

    df_expected = stormevents.load_file(resource_path('two_day_stormevents_UTC_expected.csv'),
                                        tz_localize=True)
    assert_frame_eq_ignoring_dtypes(df, df_expected)


def test_correct_tornado_times():
    df = stormevents.load_file(resource_path('stormevents_bad_times.csv'))
    df = stormevents.tors.correct_tornado_times(df)

    df_expected = stormevents.load_file(resource_path('stormevents_bad_times_corrected.csv'))
    assert_frame_eq_ignoring_dtypes(df, df_expected)


def test_after_tz_conversion_correct_tornado_times():
    df = stormevents.load_file(resource_path('stormevents_bad_times.csv'), tz='GMT')
    df = stormevents.tors.correct_tornado_times(df)

    df_expected = stormevents.load_file(resource_path('stormevents_bad_times_GMT_corrected.csv'),
                                        tz_localize=True)
    assert_frame_eq_ignoring_dtypes(df, df_expected)


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


def test_discretize_tor():
    numpts_1 = 10
    t0_1 = pd.Timestamp('2017-01-01 00:00-06:00')
    tor1 = {
        'begin_date_time': t0_1,
        'end_date_time': t0_1 + pd.Timedelta(minutes=numpts_1),
        'begin_lat': 10.00,
        'begin_lon': -100.00,
        'end_lat': 15.00,
        'end_lon': -90.00,
        'cz_timezone': 'CST',
        'event_id': 1
    }

    numpts_2 = 5
    t0_2 = pd.Timestamp('2017-01-08 00:00')
    tor2 = {
        'begin_date_time': t0_2,
        'end_date_time': t0_2 + pd.Timedelta(minutes=numpts_2),
        'begin_lat': 50.00,
        'begin_lon': -100.00,
        'end_lat': 51.00,
        'end_lon': -110.00,
        'cz_timezone': 'CST',
        'event_id': 2
    }

    t0_3 = pd.Timestamp('2017-01-09 00:00')
    tor3 = {
        'begin_date_time': t0_3,
        'end_date_time': t0_3,
        'begin_lat': 49.00,
        'begin_lon': -105.00,
        'end_lat': 49.00,
        'end_lon': -106.00,
        'cz_timezone': 'CST',
        'event_id': 3
    }

    df = pd.DataFrame({0: tor1, 1: tor2, 2: tor3}).transpose()

    points = stormevents.tors.discretize(df)

    assert points.shape[0] == numpts_1 + numpts_2 + 1

    assert points[points.event_id == 1].shape[0] == numpts_1
    for i in range(0, numpts_1):
        pt = points.loc[i]
        assert pt['timestamp'] == tor1['begin_date_time'] + pd.Timedelta(minutes=i)
        assert pt['lat'] == tor1['begin_lat'] + 0.5 * i
        assert pt['lon'] == tor1['begin_lon'] + 1 * i

    assert points[points.event_id == 2].shape[0] == numpts_2
    for i in range(0, numpts_2):
        pt = points.loc[numpts_1 + i]
        assert pt['timestamp'] == (tor2['begin_date_time'] + pd.Timedelta(minutes=i)).tz_localize('Etc/GMT+6')
        assert pt['lat'] == tor2['begin_lat'] + 0.2 * i
        assert pt['lon'] == tor2['begin_lon'] - 2 * i

    assert points[points.event_id == 3].shape[0] == 1
    extrapt = points.loc[numpts_1 + numpts_2]
    assert extrapt['timestamp'] == tor3['begin_date_time'].tz_localize('Etc/GMT+6')
    assert extrapt['lat'] == tor3['begin_lat']
    assert extrapt['lon'] == tor3['begin_lon']
