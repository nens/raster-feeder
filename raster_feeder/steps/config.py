# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the STEPS configuration file. It allows for a 'STEPS'
localconfig, too, which should be put in the same directory as this module.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import join

# central config imports
from ..config import BUILDOUT_DIR  # NOQA
from ..config import LOG_DIR       # NOQA

# storage name
NAME = 'steps'

# storage temporal depth
DEPTH = 12

# proj inferred from sample netcdf
NATIVE_GEO_TRANSFORM = -257.0, 2.0, 0.0, 255.0, 0.0, -2.0
NATIVE_PROJECTION = '+proj=gnom +lat_0=-34.264 +lon_0=150.874 +units=km'

# geographical orientation, geo_transform from auto-warped dataset using
# proj from above
WARPED_GEO_TRANSFORM = 148.004, 0.0199286, 0.0, -31.9456, 0.0, -0.0199286
WARPED_PROJECTION = 'EPSG:4326'

# -------------------------------------------
# settings to be overridden in localconfig.py
# -------------------------------------------

STORE_DIR = join(BUILDOUT_DIR, 'var', 'store')

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
