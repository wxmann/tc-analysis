import pytest
import pytz

from wxdata import _timezones as _tz


def test_get_tz_from_state():
    # all of CA is in PST
    assert _tz.tz_for_state('California') == pytz.timezone('Etc/GMT+8')

    # OR is split between PST (most of it) and MST (parts of eastern OR)
    assert _tz.tz_for_state('Oregon') is None


def test_offset_from_utc():
    assert _tz.offset_from_utc('PST') == -8
    assert _tz.offset_from_utc('PDT') == -7
    # Alaska
    assert _tz.offset_from_utc('AKST') == -9
    assert _tz.offset_from_utc('AKDT') == -8
    # GMT
    assert _tz.offset_from_utc('GMT') == 0
    # None
    assert _tz.offset_from_utc(None) == 0

    # Daylight doens't affect anything
    with pytest.raises(ValueError):
        _tz.offset_from_utc('QDT')

    # WRONG
    with pytest.raises(ValueError):
        _tz.offset_from_utc('ABC')

    # only handles short abbrevations (nothing appended)
    with pytest.raises(ValueError):
        _tz.offset_from_utc('Etc/GMT+8')


def test_parse_tz():
    # standard short abbreviations
    assert _tz.parse_tz('PST') == pytz.timezone('Etc/GMT+8')
    assert _tz.parse_tz('PDT') == pytz.timezone('Etc/GMT+7')
    assert _tz.parse_tz('AKST') == pytz.timezone('Etc/GMT+9')
    # GMT
    assert _tz.parse_tz('GMT') == pytz.timezone('GMT')
    # can directly parse
    assert _tz.parse_tz('Etc/GMT-1') == pytz.timezone('Etc/GMT-1')
    # with offsets directly appended
    assert _tz.parse_tz('CST-6') == pytz.timezone('Etc/GMT+6')
    # None
    assert _tz.parse_tz(None) == pytz.UTC

    # offset does not match abbrevation
    with pytest.raises(ValueError):
        _tz.offset_from_utc('CST+4')

    # WRONG
    with pytest.raises(ValueError):
        _tz.offset_from_utc('ABC')
