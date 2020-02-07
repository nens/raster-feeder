# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This configures a mock store for alarm trigger testing. It has global converage
and increases from -3 to 10 and back to -3, from -2 to 6 hours from now. Time
resolution and refresh interval is 5 min. Proj: WGS84. Shape: (4, 2)
"""

# central config imports
from ..config import PACKAGE_DIR  # NOQA
from ..config import LOG_DIR      # NOQA

# hour, value tuples
VALUES = [(-2, -3), (2, 10), (6, -3)]

# storage name
NAME = 'alarmtester'

# storage temporal depth
DEPTH = 97

# proj inferred from sample netcdf
GEO_TRANSFORM = -180.0, 90.0, 0.0, 90.0, 0.0, -90.0
PROJECTION = 'EPSG:4326'

# -------------------------------------------
# settings to be overridden in localconfig.py
# -------------------------------------------

# raster store location
STORE_DIR = PACKAGE_DIR / "var" / "store"

# Lizard RasterStore UUIDs to touch
TOUCH_LIZARD = []

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
