from datetime import datetime

import pytest
import pytz

from wxdata import _timezones as _tz


def test_get_tz_from_state():
    # all of CA is in PST
    assert _tz.tz_for_state('California') == pytz.timezone('Etc/GMT+8')

    # check state with space in the middle
    assert _tz.tz_for_state('New Mexico') == pytz.timezone('Etc/GMT+7')

    # OR is split between PST (most of it) and MST (parts of eastern OR)
    assert _tz.tz_for_state('Oregon') is None

    # Guam (only timezone in US territory ahead of GMT)
    assert _tz.tz_for_state('Guam') == pytz.timezone('Etc/GMT-10')


def test_parse_tz():
    # standard short abbreviations
    assert _tz.parse_tz('PST') == pytz.timezone('Etc/GMT+8')
    assert _tz.parse_tz('PDT') == pytz.timezone('Etc/GMT+7')
    assert _tz.parse_tz('AKST') == pytz.timezone('Etc/GMT+9')
    # GMT
    assert _tz.parse_tz('GMT') == pytz.timezone('GMT')
    # (these two come directly from pytz)
    assert _tz.parse_tz('GMT-0') == pytz.timezone('GMT-0')
    assert _tz.parse_tz('GMT0') == pytz.timezone('GMT0')
    # can directly parse
    assert _tz.parse_tz('Etc/GMT+1') == pytz.timezone('Etc/GMT+1')
    assert _tz.parse_tz('Etc/GMT-1') == pytz.timezone('Etc/GMT-1')
    # with offsets directly appended
    assert _tz.parse_tz('CST-6') == pytz.timezone('Etc/GMT+6')
    assert _tz.parse_tz('PDT-7') == pytz.timezone('Etc/GMT+7')
    # None
    with pytest.raises(ValueError):
        _tz.parse_tz(None)
    # Guam
    assert _tz.parse_tz('GST') == pytz.timezone('Etc/GMT-10')
    assert _tz.parse_tz('GST10') == pytz.timezone('Etc/GMT-10')

    # offset does not match abbrevation
    with pytest.raises(ValueError):
        _tz.parse_tz('CST+4')

    # WRONG
    with pytest.raises(ValueError):
        _tz.parse_tz('ABC')


def test_utc_offset_no_dst():
    # fixed TZ
    assert _tz.utc_offset_no_dst('Etc/GMT+6') == -6
    assert _tz.utc_offset_no_dst('America/Phoenix') == -7
    # DST-dependent TZ
    assert _tz.utc_offset_no_dst('America/Chicago') == -6
    assert _tz.utc_offset_no_dst('America/Los_Angeles') == -8
    assert _tz.utc_offset_no_dst('America/Los_Angeles', as_of=datetime(2017, 7, 1)) == -8
    # US abbreviations
    assert _tz.utc_offset_no_dst('CST') == -6
    assert _tz.utc_offset_no_dst('CST-6') == -6
    # convert DST to ST
    assert _tz.utc_offset_no_dst('CDT') == -6
    # GMT
    assert _tz.utc_offset_no_dst('GMT') == 0
    # Guam (Asia, on the other side of the Date Line)
    assert _tz.utc_offset_no_dst('Pacific/Guam') == 10
