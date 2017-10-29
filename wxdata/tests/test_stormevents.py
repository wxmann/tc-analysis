import os
from unittest import mock

from wxdata.stormevents import urls_for


def open_resource(filename, *args, **kwargs):
    this_dir = os.path.dirname(os.path.realpath(__file__))
    file = os.path.join(this_dir, 'resources', filename)
    return open(file, *args, **kwargs)


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
