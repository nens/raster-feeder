# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the STEPS configuration file. It allows for a 'STEPS'
localconfig, too, which should be put in the same directory as this module.
"""

# central config imports
from ..config import PACKAGE_DIR  # NOQA
from ..config import STORE_DIR  # NOQA
from ..config import LOG_DIR  # NOQA

# storage name
NAME = 'steps'

# storage temporal depth (add a frame with zero precipitation)
DEPTH = 74

# geo_transform as 6-tuple
# the second element should be positive and the last element negative
GEO_TRANSFORM = -256.0, 1.0, 0.0, 256.0, 0.0, -1.0
# Proj4 string
PROJECTION = ('+proj=aea +lat_1=-18 +lat_2=-36 '
              '+lat_0=-33.264 +lon_0=150.874 +ellps=GRS80 +units=km')
# Region of interest in EPSG:32756
ROI_ESPG32756 = 306074.77698, 6253527.45723, 319874.77698, 6265927.45723
# should result in ROI indices x 258:274, y 307:322

# remote filename strftime() format and selection pattern
FORMAT = 'IDR311EN.RF3.%Y%m%d%H%M%S.nc'
PATTERN = r'IDR311EN\.RF3\.[0-9]{14}\.nc'  # raw because invalid unicode

# Percentile number for member selection
PERCENTILE = 75

# -------------------------------------------
# settings to be overridden in localconfig.py
# -------------------------------------------

# FTP connection
FTP = dict(host='', user='', password='', path='')

# Lizard RasterStore UUIDs to touch
TOUCH_LIZARD = []

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
