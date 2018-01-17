import matplotlib.colors as colors
import matplotlib.cm as cm


def diff(df1, df2):
    return df1.merge(df2, indicator=True, how='outer')


# TODO test
def datetime_buckets(start_time, end_time, dt):
    start_bucket = start_time
    while start_bucket < end_time:
        end_bucket = start_bucket + dt
        yield start_bucket, min(end_bucket, end_time)
        start_bucket = end_bucket


def label_iter():
    alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    count = 1
    while True:
        for i in range(len(alphabet)):
            yield alphabet[i] * count
        count += 1