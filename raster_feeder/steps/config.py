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
import re

# central config imports
from ..config import BUILDOUT_DIR  # NOQA
from ..config import LOG_DIR       # NOQA

# storage name
NAME = 'steps'

# storage temporal depth (add a frame with zero precipitation)
DEPTH = 78

# proj inferred from sample netcdf
GEO_TRANSFORM = -256.0, 2.0, 0.0, 256.0, 0.0, -2.0
PROJECTION = '+proj=gnom +lat_0=-34.264 +lon_0=150.874 +units=km'

# the statistics are be performed on a region of interest (roi)
# given in 'EPSG:32756' as follows:
# 306074.77698, 319874.77698, 6253527.45723, 6265927.45723 (x1, x2, y1, y2)
#
# in the native gnomonic PROJECTION this transforms to to:
# 2.78678, 16.82551, 46.8323, 59.56076 (x1, x2, y1, y2)
#
# using the native GEO_TRANSFORM, this gives the following array indices:
STATISTICS_ROI = slice(98, 105), slice(129, 137)  # i (=y index), j (=x index)

# remote filename strftime() format and selection pattern
FORMAT = 'IDR311EN.%Y%m%d%H%M.nc'
PATTERN = re.compile('IDR311EN\.[0-9]{12}\.nc')

# -------------------------------------------
# settings to be overridden in localconfig.py
# -------------------------------------------

# FTP connection
FTP = dict(host='', user='', password='', path='')

# raster store location
STORE_DIR = join(BUILDOUT_DIR, 'var', 'store')

# Lizard RasterStore UUIDs to touch
TOUCH_LIZARD = []

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
