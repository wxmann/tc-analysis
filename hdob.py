import re
from collections import namedtuple
from datetime import datetime

import numpy as np
import pandas as pd

from common import (
    DataRetrievalException, get_links, iter_text_lines, column_definition
)

try:
    # PYTHON 2
    from StringIO import StringIO
except ImportError:
    # PYTHON 3
    from io import StringIO


def available_hdobs_between(t1, t2, storm=None, basin='atl'):
    def storm_filter(hdob_header):
        if storm is not None:
            return hdob_header.storm == storm.upper()
        return True

    for link in hdob_links_between(t1, t2, basin):
        hdob = decode_hdob(link, _header_filter=storm_filter)
        if hdob is not None:
            yield hdob


def hdob_links_between(t1, t2, basin):
    if t1.year != t2.year:
        raise ValueError("Can only fetch HDOBs within a single year")

    if basin == 'atl':
        basin_folder = 'AHONT1'
    elif basin == 'epac':
        basin_folder = 'AHOPN1'
    else:
        raise ValueError("basin argument must be either `atl` for ATLANTIC, "
                         "or `epac` for EPAC")

    year = t1.year
    hdobs_loc = 'http://www.nhc.noaa.gov/archive/recon/{}/{}/'.format(year, basin_folder)

    def hdob_timestamp(file_url):
        timestamp_match = re.search(r'\.\d{12}\.', file_url)
        if not timestamp_match:
            raise DataRetrievalException("Could not find timestamp for: {}".format(file_url))
        timestamp_str = timestamp_match.group(0)
        return datetime.strptime(timestamp_str[1:-1], '%Y%m%d%M%S')

    hdob_links = get_links(hdobs_loc, 'txt')
    for hdob_link in hdob_links:
        if t1 <= hdob_timestamp(hdob_link) < t2:
            yield hdob_link


_HDOB_COLUMNS = ['time', 'lat', 'lon', 'pressure', 'height',
                 'xtrp_sfc_pressure', 'temp', 'dewpt', 'wind',
                 'peak_fl_wind', 'sfmr_wind', 'sfmr_rain_rates', 'qc']

_HDOB_MISSING_VALUES = ['/' * n for n in range(1, 6)]

vectorqty = namedtuple('vectorqty', 'speed dir')


def decode_hdob(link, missing_value='', _header_filter=None):
    # see https://www.tropicaltidbits.com/data/NHC_recon_guide.pdf
    lines = list(iter_text_lines(link))
    header = decode_hdob_header(lines)
    if _header_filter is not None and not _header_filter(header):
        return None

    data = decode_hdob_content(lines, header, missing_value)
    return HDOB(header, data)


def decode_hdob_header(hdob_lines):
    expected_numtokens = 6
    header_line = ''
    for line in hdob_lines:
        numtokens = len(line.split())
        if numtokens == expected_numtokens:
            header_line = line
            break

    if not header_line:
        raise ValueError("Could not find HDOB header")

    components = header_line.split()
    aircraft = components[0]
    storm = components[2]
    ob_seq = int(components[4])
    start_date = components[5]

    start_date = datetime.strptime(start_date, '%Y%m%d')

    return HDOBHeader(aircraft=aircraft,
                      storm=storm,
                      ob_seq=ob_seq,
                      start_date=start_date)


def decode_hdob_content(hdob_lines, header, missing_value=''):
    contents = [line for line in hdob_lines if len(line.split()) == len(_HDOB_COLUMNS)]
    contents_stream = StringIO('\n'.join(contents))

    # TODO: handle time in next day
    def parse_datetime(val):
        return datetime.combine(header.start_date, datetime.strptime(val, '%H%M%S').time())

    converters = {
        'time': parse_datetime,
        'lat': parse_lat,
        'lon': parse_lon,
        'pressure': parse_pressure,
        'xtrp_sfc_pressure': parse_pressure,
        'temp': parse_tenths_val,
        'dewpt': parse_tenths_val,
        'wind': parse_wind
    }

    data = pd.read_csv(contents_stream, names=_HDOB_COLUMNS, delim_whitespace=True, header=None,
                       na_values=_HDOB_MISSING_VALUES, engine='python',
                       converters=converters)

    if missing_value != np.nan:
        data = data.fillna(missing_value)

    return data


HDOBHeader = namedtuple('HDOBHeader', 'aircraft storm ob_seq start_date')

columns = column_definition('HDOBColumns', _HDOB_COLUMNS)

class HDOB(object):
    def __init__(self, header, data):
        self._header = header
        self._data = data

    @property
    def header(self):
        return self._header

    @property
    def data(self):
        return self._data.copy()


# parsers for pandas dataframes -- not part of public API

def parse_lat(lat):
    deg = int(lat[:2])
    minutes = int(lat[2:-1])
    sgn = 1 if lat[-1] == 'N' else -1

    decimal = deg + minutes / 60.0
    return sgn * decimal


def parse_lon(lon):
    deg = int(lon[:3])
    minutes = int(lon[3:-1])
    sgn = 1 if lon[-1] == 'E' else -1

    decimal = deg + minutes / 60.0
    return sgn * decimal


def parse_tenths_val(val):
    return float(val) / 10


def parse_pressure(val):
    pval = parse_tenths_val(val)
    if val[0] == '0':
        pval += 1000
    return pval


def parse_wind(val):
    winddir = val[:3]
    windspd = val[3:]
    return vectorqty(speed=int(windspd), dir=int(winddir))
