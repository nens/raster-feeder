# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the NOWCAST configuration file. It allows for a 'NOWCAST'
localconfig, too, which should be put in the same directory as this module.
"""

from os.path import join

# central config imports
from ..config import BUILDOUT_DIR  # NOQA
from ..config import LOG_DIR       # NOQA

# storage name
NAME = 'nowcast-nrr'

# storage origin must be same as NRR 5min for fast group access
ORIGIN = {'year': 2000, 'month': 1, 'day': 1, 'hour': 8, 'minute': 5}
DELTA = {'minutes': 5}
DEPTH = 37

# geo
GEO_TRANSFORM = -110000, 1000, 0, 700000, 0, -1000
PROJECTION = 'EPSG:28992'

# -------------------------------------------
# settings to be overridden in localconfig.py
# -------------------------------------------

STORE_DIR = join(BUILDOUT_DIR, 'var', 'store')

# FTP connection
FTP = dict(host='', user='', password='', path='')

# Lizard RasterStore UUIDs to touch
TOUCH_LIZARD = []

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
