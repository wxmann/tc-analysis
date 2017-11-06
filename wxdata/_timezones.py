import pytz
import six


class TimeZone(object):
    @classmethod
    def todst(cls, inst):
        new_abbrev = inst.abbrev.replace('S', 'D')
        new_offset = inst.utc_offset + 1
        new_states = set(inst.full_states)
        if 'ARIZONA' in inst.full_states:
            new_states.remove('ARIZONA')
        new_inst = cls(new_abbrev, new_offset, new_states, True)
        return new_inst

    @classmethod
    def gmt(cls):
        return cls('GMT', 0, [])

    def __init__(self, abbrev, utc_offset, full_states, isdst=False):
        self.abbrev = abbrev
        self.utc_offset = utc_offset
        self.full_states = set(full_states)
        self.isdst = isdst

    def to_pytz(self):
        if self.utc_offset == 0:
            tzstr = 'GMT'
        else:
            connector = '+' if self.utc_offset < 0 else '-'
            tzstr = 'Etc/GMT{}{}'.format(connector, abs(self.utc_offset))
        return pytz.timezone(tzstr)


PST = TimeZone('PST', -8, ('WASHINGTON', 'CALIFORNIA', 'NEVADA'))
PDT = TimeZone.todst(PST)

MST = TimeZone('MST', -7, ('MONTANA', 'WYOMING', 'UTAH', 'COLORADO', 'ARIZONA', 'NEW MEXICO'))
MDT = TimeZone.todst(MST)

CST = TimeZone('CST', -6, ('OKLAHOMA', 'MINNESOTA', 'IOWA', 'WISCONSIN', 'MISSOURI', 'ARKANSAS',
                           'LOUISIANA', 'ILLINOIS', 'MISSISSIPPI', 'ALABAMA'))
CDT = TimeZone.todst(CST)

EST = TimeZone('EST', -5, ('OHIO', 'WEST VIRGINIA', 'PENNSYLVANIA', 'NEW YORK', 'VERMONT', 'NEW HAMPSHIRE',
                           'MAINE', 'MASSACHUSETTS', 'RHODE ISLAND', 'CONNECTICUT', 'NEW JERSEY', 'DELAWARE',
                           'MARYLAND', 'DISTRICT OF COLUMBIA', 'VIRGINIA', 'NORTH CAROLINA', 'SOUTH CAROLINA',
                           'GEORGIA'))
EDT = TimeZone.todst(EST)

# American Samoa, Guam, and PR/USVI do not observe DST
SST = TimeZone('SST', -11, ('AMERICAN SAMOA',))
AST = TimeZone('AST', -4, ('PUERTO RICO', 'VIRGIN ISLANDS'))
GST = TimeZone('GST', 10, ('GUAM',))

# Note: the lack of full states is because a part of the Aleutian Islands is in HST instead of AKST
AKST = TimeZone('AKST', -9, [])
AKDT = TimeZone.todst(AKST)

# Hawaii does not observe DST, but the Aleutian Islands do, so unfortunately, HDT exists.
HST = TimeZone('HST', -10, ('HAWAII',))
HDT = TimeZone.todst(HST)

GMT = TimeZone.gmt()

SUPPORTED_TIMEZONES = (
    PST, MST, CST, EST, HST, SST, AKST, AST,
    PDT, MDT, CDT, EDT, AKDT, GST, GMT
)

_state_std_tz_map = {}
for tz in (tz for tz in SUPPORTED_TIMEZONES if not tz.isdst):
    for st in tz.full_states:
        _state_std_tz_map[st] = tz


def tz_for_state(state):
    state = state.upper().strip()
    found_tz = _state_std_tz_map.get(state, None)
    if found_tz is None:
        return None
    return found_tz.to_pytz()


def tz_for_latlon(lat, lon):
    try:
        from geopy import geocoders
    except ImportError:
        import warnings
        warnings.warn("Can't find timezone from location without the `geopy` module."
                      "Please install that module.")
        raise

    g = geocoders.GoogleV3()
    found_tz = g.timezone((lat, lon))
    return found_tz


_timezone_map = {}
for tz in SUPPORTED_TIMEZONES:
    _timezone_map[tz.abbrev] = tz
    _timezone_map['{}{}'.format(tz.abbrev, tz.utc_offset)] = tz
    # support for both +0 and -0
    _timezone_map['GMT-0'] = GMT
    _timezone_map['UTC'] = GMT


def get_tz_info(abbrev):
    if abbrev is None:
        raise ValueError('Cannot get TZ info for `None`')
    tz_str = abbrev.upper().strip()
    if tz_str not in _timezone_map:
        raise ValueError("Invalid tz: {} (or offset does not match tz abbrevation)".format(tz_str))
    return _timezone_map[tz_str]


def parse_tz(tz_str):
    if tz_str is None:
        return get_tz_info(tz_str).to_pytz()
    try:
        # we're good here
        return pytz.timezone(tz_str)
    except pytz.UnknownTimeZoneError:
        # get it from our hard-coded list
        return get_tz_info(tz_str).to_pytz()


def utc_offset_no_dst(tz_str, as_of=None):
    try:
        from datetime import datetime, timedelta
        if isinstance(tz_str, six.string_types):
            tz = pytz.timezone(tz_str)
        else:
            tz = tz_str

        if as_of is None:
            as_of = datetime(datetime.now().year, 1, 1)
        dt = tz.utcoffset(as_of) - tz.dst(as_of)

        if dt < timedelta(0):
            return -24 + dt.seconds // 3600
        return dt.seconds // 3600

    except pytz.UnknownTimeZoneError:
        tz = get_tz_info(tz_str)
        return tz.utc_offset - tz.isdst
