import warnings

import pandas as pd

from wxdata import _timezones as _tzhelp

__all__ = ['convert_df_tz', 'sync_datetime_fields', 'convert_timestamp_tz']


def convert_df_tz(df, to_tz='CST', copy=True):
    assert all([
        'state' in df.columns,
        'cz_timezone' in df.columns,
        'begin_date_time' in df.columns
    ])
    if copy:
        df = df.copy()

    for col in ('begin_date_time', 'end_date_time'):
        if col in df.columns:
            df[col] = df.apply(lambda row: convert_row_tz(row, col, to_tz), axis=1)

    return sync_datetime_fields(df, to_tz)


def convert_row_tz(row, col, to_tz):
    try:
        state = row['state']
        # In older versions of storm events, `AST` and `AKST` are both logged as `AST`.
        # We can't let our tz-conversion logic believe naively it's Atlantic Standard Time
        if row['cz_timezone'] == 'AST':
            if state == 'ALASKA':
                return convert_timestamp_tz(row[col], 'AKST-9', to_tz)
            else:
                # both Puerto Rico and Virgin Islands in AST
                return convert_timestamp_tz(row[col], 'AST-4', to_tz)

        # moving on now...
        return convert_timestamp_tz(row[col], row['cz_timezone'], to_tz)
    except ValueError as e:
        # warnings.warn("Encountered an error while converting time zone, {}. "
        #               "\nAttempting to use location data to determine tz".format(repr(e)))
        try:
            tz = _tzhelp.tz_for_state(row.state)
            if tz:
                return tz.localize(row[col]).tz_convert(_pdtz_from_str(to_tz))
            else:
                lat, lon = row['begin_lat'], row['begin_lon']
                tz = _tzhelp.tz_for_latlon(lat, lon)

                # have to do some fudging here since parsed TZ from the geopy module
                # is DST-dependent. Our data is frozen onto one TZ.
                hour_offset = _tzhelp.utc_offset_no_dst(tz)
                if hour_offset < 0:
                    return convert_timestamp_tz(row[col], 'Etc/GMT+{}'.format(-hour_offset), to_tz)
                else:
                    return convert_timestamp_tz(row[col], 'Etc/GMT-{}'.format(hour_offset), to_tz)
        except KeyError as innere:
            # FIXME: KeyError is a superclass of UnknownTimeZoneError... wtf!!! Somehow handle this.
            print(repr(innere))
            warnings.warn("Can't find timezone with missing lat lon or state data.")

        raise e
    except KeyError:
        raise ValueError("Row must have `state` and `cz_timezone` column")


def convert_timestamp_tz(timestamp, from_tz, to_tz):
    original_tz_pd = _pdtz_from_str(from_tz)
    new_tz_pd = _pdtz_from_str(to_tz)
    return pd.Timestamp(timestamp, tz=original_tz_pd).tz_convert(new_tz_pd)


def _pdtz_from_str(tz_str):
    if not tz_str or tz_str in ('UTC', 'GMT'):
        import pytz
        return pytz.timezone('GMT')

    tz_str_up = tz_str.upper()
    # handle the weird cases
    if tz_str_up in ('SCT', 'CSC'):
        # found these egregious typos
        raise ValueError("{} is probably CST but cannot determine for sure".format(tz_str))
    elif tz_str_up == 'UNK':
        raise ValueError("UNK timezone")

    # we're safe; fallback to our usual timezone parsing logic
    return _tzhelp.parse_tz(tz_str)


def sync_datetime_fields(df, tz=None):

    def _extract(prefix):
        dt_col = '{}_date_time'.format(prefix)
        if dt_col in df.columns:
            dts = df[dt_col].dt

            yearmonth_col = '{}_yearmonth'.format(prefix)
            time_col = '{}_time'.format(prefix)
            day_col = '{}_day'.format(prefix)

            if yearmonth_col in df.columns:
                df[yearmonth_col] = dts.strftime('%Y%m')
            if time_col in df.columns:
                df[time_col] = dts.strftime('%H%M')
            if day_col in df.columns:
                df[day_col] = dts.day

            if prefix == 'begin' and 'year' in df.columns:
                df['year'] = dts.year

            if prefix == 'begin' and 'month_name' in df.columns:
                df['month_name'] = dts.strftime('%B')

    _extract('begin')
    _extract('end')

    if tz is not None:
        df['cz_timezone'] = tz

    return df
