#!/usr/bin/env python

from collections import namedtuple
from datetime import date, datetime, timedelta
from decimal import Decimal
import math
import sys
import warnings

import six

try:
    __IPYTHON__
    from IPython.display import SVG as IPythonSVG
except (NameError, ImportError):
    IPythonSVG = lambda x: x


# Shorthand
ZERO = Decimal('0')
NINE_PLACES = Decimal('1e-9')

#: X data dimension index
X = 0

#: Y data dimension index
Y = 1

#: Z data dimension index
Z = 2


DIMENSION_NAMES = ['X', 'Y', 'Z']

#: Data structure for representing margins or other CSS-edge like properties
Box = namedtuple('Box', ['top', 'right', 'bottom', 'left'])

#: Data structure for a single series data point
Datum = namedtuple('Datum', ['i', 'x', 'y', 'z', 'row'])

#: Dummy object used in place of a series when rendering legends for categories
DummySeries = namedtuple('DummySeries', ['name'])


formatwarning_orig = warnings.formatwarning
warnings.formatwarning = lambda message, category, filename, lineno, line=None: \
    formatwarning_orig(message, category, filename, lineno, line='')

warn = warnings.warn
warnings.resetwarnings()
warnings.simplefilter('always')


# In Python 3.5 use builtin C implementation of `isclose`
if sys.version_info >= (3, 5):
    from math import isclose
else:
    def isclose(a, b, rel_tol=NINE_PLACES, abs_tol=ZERO):
        """
        Test if two floating points numbers are close enough to be considered
        equal.

        Via: https://github.com/PythonCHB/close_pep/blob/master/isclose.py

        Verified against final CPython 3.5 implementation.

        :param a:
            The first number to check.
        :param b:
            The second number to check.
        :param rel_tol:
            Relative tolerance. The amount of error allowed, relative to the larger
            input value. Defaults to nine decimal places of accuracy.
        :param abs_tol:
            Absolute minimum tolerance. Disabled by default.
        """
        if a == b:
            return True

        if rel_tol < ZERO or abs_tol < ZERO:
            raise ValueError('Tolerances must be non-negative')

        if math.isinf(abs(a)) or math.isinf(abs(b)):
            return False

        diff = abs(b - a)

        return (((diff <= abs(rel_tol * b)) or
                (diff <= abs(rel_tol * a))) or
                (diff <= abs_tol))


def to_year_count(d):
    """
    date > n years
    """
    return d.year


def from_year_count(n, t=date):
    """
    n years > date
    """
    return t(n, 1, 1)


def to_month_count(d):
    """
    date > n months
    """
    return (d.year * 12) + d.month


def from_month_count(n, t=date):
    """
    n months > date
    """
    return t(n // 12, (n % 12) + 1, 1)


def to_day_count(d):
    """
    date > n days
    """
    return (d - type(d).min).days


def from_day_count(n, t=date):
    """
    n days > date
    """
    return t.min + timedelta(days=n)


def to_hour_count(d):
    """
    date > n hours
    """
    return (d - datetime.min).total_seconds() / (60 * 60)


def from_hour_count(n, t=datetime):
    """
    n hours > date
    """
    return t.min + timedelta(hours=n)


def to_minute_count(d):
    """
    date > n minutes
    """
    return (d - datetime.min).total_seconds() / 60


def from_minute_count(n, t=datetime):
    """
    n minutes > date
    """
    return t.min + timedelta(minutes=n)


def to_second_count(d):
    """
    date > n seconds
    """
    return (d - datetime.min).total_seconds()


def from_second_count(n, t=datetime):
    """
    n seconds > date
    """
    return t.min + timedelta(seconds=n)


def to_microsecond_count(d):
    """
    date > n microseconds
    """
    return (d - datetime.min).total_seconds() * 1000


def from_microsecond_count(n, t=datetime):
    """
    n microseconds > date
    """
    return t.min + timedelta(microseconds=n)
