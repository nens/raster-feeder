# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import os

from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

from . import config


# path helpers
class PathHelper(object):
    """
        >>> import datetime
        >>> dt = datetime.datetime(2011, 03, 05, 14, 15, 00)
        >>> ph = PathHelper('a', 'c', '{code}:{timestamp}')
        >>> ph. path(dt)
        u'a/c/2011/03/05/c:20110305141500'
    """

    TIMESTAMP_FORMAT = config.TIMESTAMP_FORMAT

    def __init__(self, basedir, code, template):
        """
        Filetemplate must be something like '{code}_{timestamp}.'
        """
        self._basedir = os.path.join(basedir, code)
        self._code = code
        self._template = template

    def path(self, dt):
        return os.path.join(
            self._basedir,
            dt.strftime('%Y'),
            dt.strftime('%m'),
            dt.strftime('%d'),
            self._template.format(
                code=self._code,
                timestamp=dt.strftime(self.TIMESTAMP_FORMAT)
            )
        )

    def path_with_hour(self, dt):
        return os.path.join(
            self._basedir,
            dt.strftime('%Y'),
            dt.strftime('%m'),
            dt.strftime('%d'),
            dt.strftime('%H'),
            self._template.format(
                code=self._code,
                timestamp=dt.strftime(self.TIMESTAMP_FORMAT)
            )
        )


# Timing
def closest_time(timeframe='f', dt_close=None):
    '''
    Get corresponding datetime based on the timeframe.
    e.g. with    dt_close = 2012-04-13 12:27
    timeframe = 'f' returns 2012-04-13 12:25
    timeframe = 'h' returns 2012-04-13 12:00
    timeframe = 'd' returns 2012-04-12 08:00
    '''
    if dt_close:
        now = dt_close
    else:
        now = Datetime.utcnow()

    if timeframe == 'h':
        closesttime = now.replace(minute=0, second=0, microsecond=0)
    elif timeframe == 'd':
        closesttime = now.replace(hour=8, minute=0, second=0, microsecond=0)
        if closesttime > now:
            closesttime = closesttime - Timedelta(days=1)
    else:
        closesttime = now.replace(
            minute=now.minute - (now.minute % 5), second=0, microsecond=0,
        )
    return closesttime


def get_valid_timeframes(datetime):
    """ Return a list of timeframe codes corresponding to a datetime."""
    result = []
    if datetime.second == 0 and datetime.microsecond == 0:
        if datetime.minute == (datetime.minute // 5) * 5:
            result.append('f')
        if datetime.minute == 0:
            result.append('h')
            if datetime.hour == 8:
                result.append('d')
    return result


def consistent_product_expected(prodcode, timeframe):
    """
    Return if a consistent product is expected, for a product. It
    can be used to determine if a product needs to be published, or
    not. If not, the rescaled equivalent should be published.
    """
    if prodcode in 'au' and timeframe in 'fh':
        return True
    if prodcode == 'n' and timeframe == 'f':
        return True
    return False


def get_geo_transform():
    left, right, top, bottom = config.COMPOSITE_EXTENT
    width, height = config.COMPOSITE_CELLSIZE
    return left, width, 0, top, 0, -height
