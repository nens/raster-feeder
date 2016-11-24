# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import abspath, dirname, join

import datetime

BUILDOUT_DIR = abspath(join(dirname(__file__), '..', '..'))

# directories
LOG_DIR = join(BUILDOUT_DIR, 'var', 'log')

# data is read from here
CALIBRATE_DIR = join(BUILDOUT_DIR, 'var', 'calibrate')
CONSISTENT_DIR = join(BUILDOUT_DIR, 'var', 'consistent')

# where to put the stores
STORE_DIR = join(BUILDOUT_DIR, 'var', 'store')

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

# Naming of products and files
FRAMESTAMP = dict(f='0005', h='0100', d='2400')
PRODUCT_CODE = {t: {p: 'TF{}_{}'.format(FRAMESTAMP[t], p.upper())
                    for p in 'rnau'}
                for t in 'fhd'}
PRODUCT_TEMPLATE = 'RAD_{code}_{timestamp}.h5'

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
