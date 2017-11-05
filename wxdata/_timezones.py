import pytz

_timezone_map = dict()

for state in ('WASHINGTON', 'CALIFORNIA', 'NEVADA'):
    _timezone_map[state] = 'PST'

for state in ('MONTANA', 'WYOMING', 'UTAH', 'COLORADO', 'ARIZONA', 'NEW MEXICO'):
    _timezone_map[state] = 'MDT'

for state in ('OKLAHOMA', 'MINNESOTA', 'IOWA', 'WISCONSIN', 'MISSOURI', 'ARKANSAS',
              'LOUISIANA', 'ILLINOIS', 'MISSISSIPPI', 'ALABAMA'):
    _timezone_map[state] = 'CST'

for state in ('OHIO', 'WEST VIRGINIA', 'PENNSYLVANIA', 'NEW YORK', 'VERMONT', 'NEW HAMPSHIRE',
              'MAINE', 'MASSACHUSETTS', 'RHODE ISLAND', 'CONNECTICUT', 'NEW JERSEY', 'DELAWARE',
              'MARYLAND', 'DISTRICT OF COLUMBIA', 'VIRGINIA', 'NORTH CAROLINA', 'SOUTH CAROLINA',
              'GEORGIA'):
    _timezone_map[state] = 'EST'

for state in ('HAWAII', 'GUAM'):
    _timezone_map[state]= 'HST'

for state in ('AMERICAN SAMOA',):
    _timezone_map[state] = 'SST'

for state in ('PUERTO RICO', 'VIRGIN ISLANDS'):
    _timezone_map[state] = 'AST'


def tz_for_state(st):
    tzstr = _timezone_map.get(st.upper().strip(), None)
    if tzstr is None:
        return None
    return parse_tz(tzstr)


def tz_for_latlon(lat, lon):
    from geopy import geocoders
    g = geocoders.GoogleV3()

    tz = g.timezone((lat, lon))
    return tz


def parse_tz(tz_str):
    if not tz_str:
        return pytz.UTC
    try:
        # we're good here
        return pytz.timezone(tz_str)
    except pytz.UnknownTimeZoneError:
        # got to handle abbrevations manually
        try:
            # 'CST', 'MDT', etc..
            offset = offset_from_utc(tz_str)
        except ValueError:
            # if delta is explicitly specified, CST-6, EST-5
            try:
                abbrev, hr_delta = tz_str.split('-')[:2]
            except ValueError:
                raise ValueError("Invalid tz: {}".format(tz_str))

            hr_delta = -int(hr_delta)
            offset = offset_from_utc(abbrev)
            if offset != hr_delta:
                raise ValueError("Time zone does not agree with offset: {}".format(tz_str))
        if offset < 0:
            return pytz.timezone('Etc/GMT+{}'.format(-offset))
        else:
            return pytz.timezone('Etc/GMT-{}'.format(offset))


_tz_offset_map = {
    'SST': -11,
    'HST': -10,
    'AKST': -9,
    'PST': -8,
    'MST': -7,
    'CST': -6,
    'EST': -5,
    'AST': -4
}


def offset_from_utc(abbrev):
    if not abbrev or abbrev.upper() in ('UTC', 'GMT'):
        return 0

    abbrev = abbrev.upper().strip()
    try:
        offset = _tz_offset_map.get(abbrev, None)
        if offset is None:
            # see if it's DST. If this also fails, we have an invalid timezone abbreviation
            offset = _tz_offset_map[abbrev.replace('D', 'S')] + 1
        return offset
    except KeyError:
        raise ValueError("Invalid tz: {}".format(abbrev))