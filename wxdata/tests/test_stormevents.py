import os
from itertools import product
from unittest import mock

import pandas as pd
import pytz
from pandas.util.testing import assert_frame_equal

from wxdata import stormevents, _timezones
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


@mock.patch('wxdata._timezones.tz_for_latlon', return_value=pytz.timezone('Etc/GMT+5'))
def test_convert_df_timezone(latlontz):
    src_df = stormevents.load_file(resource_path('stormevents_mixed_tzs.csv'))
    src_df = src_df[['begin_yearmonth', 'begin_day', 'begin_time', 'end_yearmonth',
                     'end_day', 'end_time', 'state', 'year', 'month_name', 'event_type',
                     'cz_name', 'cz_timezone', 'begin_date_time', 'end_date_time',
                     'begin_lat', 'begin_lon', 'episode_narrative', 'event_narrative']]

    expected_df = _load_localizing_timezones(resource_path('stormevents_mixed_tzs_togmt.csv'))
    converted_src_df = stormevents.convert_df_tz(src_df, 'GMT')
    assert_frame_equal(converted_src_df, expected_df)


def _load_localizing_timezones(file):
    df = stormevents.load_file(file)
    df['begin_date_time'] = df.apply(lambda row: _timezones.parse_tz(row['cz_timezone']).localize(row['begin_date_time']),
                                     axis=1)
    df['end_date_time'] = df.apply(lambda row: _timezones.parse_tz(row['cz_timezone']).localize(row['end_date_time']),
                                   axis=1)
    return df
