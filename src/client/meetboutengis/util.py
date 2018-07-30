import datetime
from pandas import pandas


def as_decimal(value):
    return str(value).strip().replace(",", ".") if not pandas.isnull(value) else None


def as_str(value):
    return str(value) if not pandas.isnull(value) else None


def as_date(value, format):
    return datetime.datetime.strptime(str(value), format).date().strftime("%Y-%m-%d")\
        if not pandas.isnull(value) else None
