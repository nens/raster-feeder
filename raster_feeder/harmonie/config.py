# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the HARMONIE configuration file. It allows for a 'HARMONIE'
localconfig, too, which should be put in the same directory as this module.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

# central config imports
from ..config import BUILDOUT_DIR  # NOQA
from ..config import STORE_DIR     # NOQA
from ..config import LOG_DIR       # NOQA

from ..config import REDIS_HOST    # NOQA
from ..config import REDIS_DB      # NOQA

# Default nodatavalue
NODATAVALUE = -9999

# Store and group names
GROUP_NAME = 'harmonie'
STORE_NAMES = 'harmonie1', 'harmonie2'

# Geographical orientation
GEO_TRANSFORM = -0.0185, 0.037, 0, 55.8885, 0, -0.023
PROJECTION = 'EPSG:4326'

# remote file name strftime() format
FORMAT = 'harm36_v1_ned_surface_%Y%m%d%H.tgz'

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
