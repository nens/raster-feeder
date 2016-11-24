# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import datetime
import os

BUILDOUT_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    '..',
)

# directories
LOG_DIR = os.path.join(BUILDOUT_DIR, 'var', 'log')

# data is read from here
CALIBRATE_DIR = None
CONSISTENT_DIR = None

# where to put the stores
STORE_DIR = None

# Default nodatavalue
NODATAVALUE = -9999

# Gridproperties for resulting composite (left, right, top, bottom)
COMPOSITE_EXTENT = (-110000, 390000, 700000, 210000)
COMPOSITE_CELLSIZE = (1000, 1000)

# redis host for mtime cache and turn locking system
REDIS_HOST = 'localhost'
REDIS_DB = 0

# Format for all-digit timestamp
TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'

# Delivery times for various products (not a dict, because order matters)
DELIVERY_TIMES = (
    ('x', datetime.timedelta()),
    ('r', datetime.timedelta()),
    ('n', datetime.timedelta(hours=1)),
    ('a', datetime.timedelta(hours=12)),
    ('u', datetime.timedelta(days=30)),
)

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
