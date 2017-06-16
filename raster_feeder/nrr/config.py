# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the NRR configuration file. It allows for a 'NRR' localconfig, too,
which should be put in the same directory as this module.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import join
from datetime import timedelta as Timedelta

# central config imports
from ..config import BUILDOUT_DIR
from ..config import LOG_DIR       # NOQA

from ..config import REDIS_HOST    # NOQA
from ..config import REDIS_DB      # NOQA

# data is read from here
from ..config import STORE_DIR     # NOQA
CALIBRATE_DIR = join(BUILDOUT_DIR, 'var', 'calibrate')
CONSISTENT_DIR = join(BUILDOUT_DIR, 'var', 'consistent')

# Default nodatavalue
NODATAVALUE = -9999

# Geographical orientation
GEO_TRANSFORM = -110000, 1000, 0, 700000, 0, -1000
PROJECTION = 'EPSG:28992'

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
    ('x', Timedelta()),
    ('r', Timedelta()),
    ('n', Timedelta(hours=1)),
    ('a', Timedelta(hours=12)),
    ('u', Timedelta(days=30)),
)

# -------------------------------------------
# settings to be overridden in localconfig.py
# -------------------------------------------

# report script
REPORT_SMTP_HOST = ''
REPORT_SENDER = ''
REPORT_RECIPIENTS = []

# nowcast FTP connection
FTP_NOWCAST = dict(host='', user='', password='', path='')


# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
