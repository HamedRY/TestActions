from datetime import datetime
from dateutil import tz
import numpy as np
import warnings
import functools

def deprecated(func):
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)
        return func(*args, **kwargs)
    return new_func

def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate

@static_vars(from_zone=tz.tzutc(), to_zone=tz.gettz('Europe/Amsterdam'))
def utc_ts_to_local_str(timestamp):
    if timestamp is None:
        return ''
    return datetime.fromtimestamp(timestamp).\
        replace(tzinfo=utc_ts_to_local_str.from_zone).\
        astimezone(utc_ts_to_local_str.to_zone).\
        strftime('%Y-%m-%d %H:%M:%S')

def subtract_datasets(a, b):
    a = [tuple(sorted(d.items())) for d in a]
    b = [tuple(sorted(d.items())) for d in b]
    return [dict(kvs) for kvs in set(a).difference(b)]

def filter_outliers(list_of_numbers):
    list_of_numbers.sort()
    q1 = np.quantile(list_of_numbers,0.25)
    q3 = np.quantile(list_of_numbers,0.75)
    iqr = q3 - q1
    max_value = q3 + iqr * 1.5
    min_value = q1 - iqr * 1.5
    outliers = []
    filtered_list = []
    for item in list_of_numbers:
        if min_value <= item <= max_value:
            filtered_list.append(item)
        else :
            outliers.append(item)
    if len(outliers) > 0:
        return {'max_axis_break':min(outliers), 'min_axis_break':max(filtered_list)}
    return None
    
def chunks(list, size):
    for i in range(0, len(list), size):
        yield list[i:i + size]

def get_trendline(values):
    trend = range(len(values))
    size = len(trend)
    Sx = Sy = Sxx = Syy = Sxy = 0.0
    for x, y in zip(trend, values):
        Sx = Sx + x
        Sy = Sy + y
        Sxx = Sxx + x * x
        Syy = Syy + y * y
        Sxy = Sxy + x * y
    det = Sxx * size - Sx * Sx
    if det == 0:
        return values
    a, b = (Sxy * size - Sy * Sx) / det, (Sxx * Sy - Sx * Sxy) / det
    return [a * index + b for index in trend]